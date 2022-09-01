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
package io.confluent.kafka.schemaregistry.rules.jsonata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSortedMap;
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
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.sql.Ref;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

public class JsonataExecutorTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final KafkaAvroSerializer reflectionAvroSerializer;
  private final KafkaAvroDeserializer reflectionAvroDeserializer;
  private final String topic;

  public JsonataExecutorTest() {
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, "false");
    defaultConfig.put(KafkaAvroSerializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v1");
    defaultConfig.put(KafkaAvroSerializerConfig.LATEST_COMPATIBILITY_STRICT, "false");
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    topic = "test";

    HashMap<String, String> reflectionProps = new HashMap<String, String>();
    // Intentionally invalid schema registry URL to satisfy the config class's requirement that
    // it be set.
    reflectionProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    reflectionProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "false");
    reflectionProps.put(KafkaAvroDeserializerConfig.USE_LATEST_WITH_METADATA,
        "application.version=v2");
    reflectionProps.put(KafkaAvroDeserializerConfig.LATEST_COMPATIBILITY_STRICT, "false");
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

  @Test
  public void testKafkaAvroSerializerReflectionRecord() throws Exception {
    byte[] bytes;
    Object obj;

    String ruleString =
        "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

    reflectionAvroDeserializer.registerMessageTransform(JsonataExecutor.TYPE, new JsonataExecutor());

    OldWidget widget = new OldWidget("alice");
    Schema schema = ReflectData.get().getSchema(OldWidget.class);
    AvroSchema avroSchema = new AvroSchema(schema);
    SortedMap<String, String> props = ImmutableSortedMap.of("application.version", "v1");
    Metadata metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, RuleSet.EMPTY_RULESET);
    schemaRegistry.register(topic + "-value", avroSchema);

    schema = ReflectData.get().getSchema(NewWidget.class);
    avroSchema = new AvroSchema(schema);
    Rule rule = new Rule("myRule", RuleKind.MESSAGE_XFORM, RuleMode.UPGRADE,
        JsonataExecutor.TYPE, ruleString);
    RuleSet ruleSet = new RuleSet(Collections.singletonList(rule), Collections.emptyList());
    props = ImmutableSortedMap.of("application.version", "v2");
    metadata = new Metadata(Collections.emptySortedMap(), props, Collections.emptySortedSet());
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    bytes = reflectionAvroSerializer.serialize(topic, widget);

    obj = reflectionAvroDeserializer.deserialize(topic, bytes);
    assertTrue(
        "Returned object should be a NewWidget",
        NewWidget.class.isInstance(obj)
    );
  }

  public static class OldWidget {
    private String name;
    private int size;
    private int version;

    public OldWidget() {}
    public OldWidget(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getSize() {
      return size;
    }

    public void setSize(int size) {
      this.size = size;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OldWidget widget = (OldWidget) o;
      return name.equals(widget.name)
          && size == widget.size
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, size, version);
    }
  }

  public static class NewWidget {
    private String name;
    private int height;
    private int version;

    public NewWidget() {}
    public NewWidget(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getHeight() {
      return height;
    }

    public void setHeight(int height) {
      this.height = height;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NewWidget widget = (NewWidget) o;
      return name.equals(widget.name)
          && height == widget.height
          && version == widget.version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, height, version);
    }
  }
}
