/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.encryption.hcvault;

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.EMPTY_AAD;
import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.TEST_CLIENT;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultFieldEncryptionExecutor.KMS_KEY_ID;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultFieldEncryptionExecutor.TOKEN_ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.api.Logical;
import com.bettercloud.vault.response.LogicalResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class HcVaultFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final String topic;

  public HcVaultFieldEncryptionExecutorTest() throws Exception {
    topic = "test";
    String keyId = "http://127.0.0.1:8200/transit/keys/my-key";
    Vault testClient = mockClient(keyId);
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS, "hcvault");
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.class",
        HcVaultFieldEncryptionExecutor.class.getName());
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.param." + KMS_KEY_ID,
        keyId);
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.param." + TOKEN_ID,
        "dev-only-token");
    defaultConfig.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.param." + TEST_CLIENT,
        testClient);
    schemaRegistry = new MockSchemaRegistryClient();

    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, new HashMap(defaultConfig));

    HashMap<String, Object> reflectionProps = new HashMap<>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS, "hcvault");
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.class",
        HcVaultFieldEncryptionExecutor.class.getName());
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.param." + KMS_KEY_ID,
        keyId);
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.param." + TOKEN_ID,
        "dev-only-token");
    reflectionProps.put(KafkaAvroDeserializerConfig.RULE_EXECUTORS + ".hcvault.param." + TEST_CLIENT,
        testClient);
    reflectionAvroSerializer = new KafkaAvroSerializer(schemaRegistry, reflectionProps);
    reflectionAvroDeserializer = new KafkaAvroDeserializer(schemaRegistry, reflectionProps);

  }

  private Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": \"string\", "
        + "\"confluent.annotations\": [\"PII\"]}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private IndexedRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  @Test
  public void testKafkaAvroSerializer() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("myRule", null, null,
        HcVaultFieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = new Metadata(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", record.get("name"));
  }

  private static Vault mockClient(String keyId) throws Exception {
    Aead aead = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead.class);
    Map<String, String> response = new HashMap<>();
    LogicalResponse logicalResponse = mock(LogicalResponse.class);
    when(logicalResponse.getData()).thenReturn(response);
    Logical logical = mock(Logical.class);
    Vault client = mock(Vault.class);
    when(client.logical()).thenReturn(logical);
    when(logical.write(any(String.class), any(Map.class)))
        .thenAnswer(invocationOnMock -> {
          String path = invocationOnMock.getArgument(0);
          Map<String, Object> request = invocationOnMock.getArgument(1);
          if (request.containsKey("plaintext")) {
            byte[] plaintext = Base64.getDecoder().decode(((String) request.get("plaintext")));
            byte[] ciphertext = aead.encrypt(plaintext, EMPTY_AAD);
            response.put("ciphertext", Base64.getEncoder().encodeToString(ciphertext));
          } else {
            byte[] ciphertext = Base64.getDecoder().decode(((String) request.get("ciphertext")));
            byte[] plaintext = aead.decrypt(ciphertext, EMPTY_AAD);
            response.put("plaintext", Base64.getEncoder().encodeToString(plaintext));
          }
          return logicalResponse;
        });
    return client;
  }
}

