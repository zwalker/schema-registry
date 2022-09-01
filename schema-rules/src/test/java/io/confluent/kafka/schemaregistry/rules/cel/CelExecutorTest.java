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
package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import java.util.Objects;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

public class CelExecutorTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final String topic;

  public CelExecutorTest() {
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
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"User\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
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

  private Schema createWidgetSchema() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"Widget\","
        + "\"namespace\":\"io.confluent.kafka.schemaregistry.rules.cel.CelExecutorTest\","
        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\", "
        + "\"confluent.annotations\": [\"PII\"]}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(schemaStr);
    return schema;
  }

  @Test
  public void testKafkaAvroSerializer() throws Exception {
    avroSerializer.registerMessageTransform(CelExecutor.TYPE, new CelExecutor());

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(avroRecord.getSchema());
    Rule rule = new Rule("myRule", RuleKind.CONSTRAINT, RuleMode.READ,
        CelExecutor.TYPE, "message.name == \"testUser\"");
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(Metadata.EMPTY_METADATA, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer.deserialize(topic, bytes));
  }

  @Test
  public void testKafkaAvroSerializerReflection() throws Exception {
    byte[] bytes;
    Object obj;

    reflectionAvroSerializer.registerMessageTransform(CelExecutor.TYPE, new CelExecutor());

    Widget widget = new Widget("alice");
    Schema schema = ReflectData.get().getSchema(widget.getClass());
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.CONSTRAINT, RuleMode.READ,
        CelExecutor.TYPE, "message.name == \"alice\"");
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(Metadata.EMPTY_METADATA, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);


    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object should be a Widget",
        Widget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
  }

  @Test
  public void testKafkaAvroSerializerReflectionFieldTransform() throws Exception {
    byte[] bytes;
    Object obj;

    reflectionAvroSerializer.registerFieldTransformProvider(CelExecutor.TYPE, new CelExecutor());

    Widget widget = new Widget("alice");
    Schema schema = createWidgetSchema();
    AvroSchema avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.FIELD_XFORM, RuleMode.WRITE,
        CelExecutor.TYPE, "value + \"-suffix\"");
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), Collections.singletonList(rule));
    avroSchema = avroSchema.copy(Metadata.EMPTY_METADATA, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);


    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes, schema);
    assertTrue(
        "Returned object should be a Widget",
        Widget.class.isInstance(obj)
    );
    assertEquals(widget, obj);
    assertEquals("alice-suffix", ((Widget)obj).getName());
  }

  public static class Widget {
    private String name;

    public Widget() {}
    public Widget(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Widget widget = (Widget) o;
      return name.equals(widget.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }
}
