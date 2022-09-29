/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.rules.encryption;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class FieldEncryptionExecutorTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final String topic;

  public FieldEncryptionExecutorTest() {
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    topic = "test";

    HashMap<String, String> reflectionProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
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
    avroSerializer.registerFieldTransformProvider(
        FieldEncryptionExecutor.TYPE, new FieldEncryptionExecutor());
    avroDeserializer.registerFieldTransformProvider(
        FieldEncryptionExecutor.TYPE, new FieldEncryptionExecutor());

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("myRule", RuleKind.FIELD_XFORM, RuleMode.READWRITE,
        FieldEncryptionExecutor.TYPE, "PII");
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = new Metadata(
        Collections.emptyMap(), getConfiguration(), Collections.emptySet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", record.get("name"));
  }

  private Map<String, String> getConfiguration() {
    Map<String, String> config = new HashMap<>();

    //config.put("schema.registry.url", "mock://test");
    config.put("encryption.provider.name", "cachedkeyprovider");

    config.put("cachedkeyprovider.provider.class",
        "io.confluent.encryption.common.crypto.cipher.impl.CachedCipherProvider");
    config.put("cachedkeyprovider.provider.expiry", "3600");
    config.put("cachedkeyprovider.provider.name", "local");

    config.put("local.provider.class",
        "io.confluent.encryption.common.crypto.cipher.impl.LocalCipherProvider");
    config.put("local.provider.keys", "EncryptedKey,APrivateKey,APublicKey");
    config.put("local.provider.EncryptedKey.key", "hPQTN5fVUirh6Ec1uvbiXQ==");
    config.put("local.provider.EncryptedKey.key.type", "SymmetricKey");
    config.put("local.provider.APrivateKey.key",
        "MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBALKhoodcCNOWsSL+Fhb/BZenYUOP/bdb66RtpYhocwdT4PqR3IvVGcSt8FAfbtuTYzjxUK9MyamHM1uTv3a3vLQWZqbTdxzbT3wNE90ujE8ghFEDMqXPqBUeuOdTqJOu7naXRaEhff0UsxkNPNW/KJ7EBcPlgyRYHq0GCFQK8HLZAgMBAAECgYA63xhwof1qtoxUqwbet1fBfnGI0cjdiFbmDxjyFvJqJNPN4QqdrzLZ5jWAQovHpBCccLOVwqnnzF45vTzpxG5VIjKJpahZcshdg1GbihnpGUzVtwcJ3linByDDAML72QwXYpJZqJrGfLSuN3zNKbA4+8vggE22yTWk7zXa+DQazQJBAOwlBkG23Ex/OqQZ2TWPW5alUJmUeCC7aKdUZ1e/au1qUy29DEZTefhEDmCU27TvyhWOMt55F6MFlFKyo+JdtocCQQDBpqWm1iFldnJE97S7HPxp28oYOvmffIupWlpMjf7DMy5qTpvQYxgsSk/g7M7I4xIZtZAZ0qBy0HaY0sm/Q4OfAkEA2uvUKhXxlAWbgsjn4sydl0J5P3gyCf5UHlSUXff6lFGu/Uc22vfGqo/FWGqIaOyox2UF6dQPQrYIdMZiQpiofwJAJ26ospVNzZxV3mdWPPfFCkVAHLj9lZVF1yFX29jaNKNaYzlIjyFuja5AH7v4y305dVS8WBXEqDx8udfKTxEPXwJAeH4rE2Bep5OyUT7W+PUas+Qc3YVUDVwvu6oP5CdcxI+6OQtlA3EX3sVfhnCC53N4R85JGkksrwnwRz1zN8vPxw==");
    config.put("local.provider.APrivateKey.key.type", "PrivateKey");
    config.put("local.provider.APublicKey.key",
        "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCyoaKHXAjTlrEi/hYW/wWXp2FDj/23W+ukbaWIaHMHU+D6kdyL1RnErfBQH27bk2M48VCvTMmphzNbk792t7y0Fmam03cc2098DRPdLoxPIIRRAzKlz6gVHrjnU6iTru52l0WhIX39FLMZDTzVvyiexAXD5YMkWB6tBghUCvBy2QIDAQAB");
    config.put("local.provider.APublicKey.key.type", "PublicKey");
    config.put("local.provider.default.key", "EncryptedKey");

    return config;
  }
}
