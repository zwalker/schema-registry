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

package io.confluent.kafka.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.FieldTransformProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.JsonMapper;
import io.confluent.kafka.schemaregistry.rules.MessageFieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.MessageTransform;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.serializers.context.NullContextNameStrategy;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.kafka.common.header.Headers;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaSchemaSerDe implements JsonMapper {

  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int idSize = 4;
  protected static final int DEFAULT_CACHE_CAPACITY = 1000;

  protected SchemaRegistryClient schemaRegistry;
  protected ContextNameStrategy contextNameStrategy = new NullContextNameStrategy();
  protected Object keySubjectNameStrategy = new TopicNameStrategy();
  protected Object valueSubjectNameStrategy = new TopicNameStrategy();
  protected Map<SubjectSchema, ParsedSchema> latestVersions =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  protected Map<String, ParsedSchema> latestWithMetadata =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  protected boolean useSchemaReflection;
  protected Map<String, String> metadata;
  protected Map<String, MessageTransform> messageTransforms = new ConcurrentHashMap<>();
  protected Map<String, FieldTransformProvider> fieldTransformProviders = new ConcurrentHashMap<>();
  protected boolean isKey;

  protected void configureClientProperties(
      AbstractKafkaSchemaSerDeConfig config,
      SchemaProvider provider) {
    if (schemaRegistry == null) {
      List<String> urls = config.getSchemaRegistryUrls();
      int maxSchemaObject = config.getMaxSchemasPerSubject();
      Map<String, Object> originals = config.originalsWithPrefix("");
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          urls,
          maxSchemaObject,
          Collections.singletonList(provider),
          originals,
          config.requestHeaders()
      );
    }

    contextNameStrategy = config.contextNameStrategy();
    keySubjectNameStrategy = config.keySubjectNameStrategy();
    valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    useSchemaReflection = config.useSchemaReflection();
    if (config.getLatestWithMetadataSpec() != null) {
      MapPropertyParser parser = new MapPropertyParser();
      metadata = parser.parse(config.getLatestWithMetadataSpec());
    }
  }

  public boolean isKey() {
    return isKey;
  }

  protected ParsedSchema getLatestWithMetadata(String subject)
      throws IOException, RestClientException {
    if (metadata == null || metadata.isEmpty()) {
      return null;
    }
    ParsedSchema schema = latestWithMetadata.get(subject);
    if (schema == null) {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestWithMetadata(subject, metadata, true);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      schema = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      schema = schema.copy(schemaMetadata.getVersion());
      latestWithMetadata.put(subject, schema);
    }
    return schema;
  }

  private ParsedSchema getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    SchemaMetadata schemaMetadata = schemaRegistry.getSchemaMetadata(subject, version, true);
    Optional<ParsedSchema> optSchema =
        schemaRegistry.parseSchema(
            new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                null, schemaMetadata));
    ParsedSchema schema = optSchema.orElseThrow(
        () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
            + " with refs " + schemaMetadata.getReferences()
            + " of type " + schemaMetadata.getSchemaType()));
    return schema.copy(schemaMetadata.getVersion());
  }

  protected List<Migration> getMigrations(
      String subject, ParsedSchema writerSchema, ParsedSchema latestWithMetadata)
      throws IOException, RestClientException {
    RuleMode migrationMode = null;
    if (writerSchema.version() < latestWithMetadata.version()) {
      migrationMode = RuleMode.UPGRADE;
    } else if (writerSchema.version() > latestWithMetadata.version()) {
      migrationMode = RuleMode.DOWNGRADE;
    }

    List<Migration> migrations = new ArrayList<>();
    if (migrationMode != null) {
      List<ParsedSchema> versions = getSchemasBetween(subject, writerSchema, latestWithMetadata);
      if (versions.size() <= 1) {
        return migrations;
      }
      ParsedSchema previous = null;
      for (int i = 0; i < versions.size(); i++) {
        ParsedSchema current = versions.get(i);
        if (i == 0 && migrationMode == RuleMode.UPGRADE) {
          // during upgrades, skip the first schema
          previous = current;
          continue;
        } else if (i == versions.size() - 1 && migrationMode == RuleMode.DOWNGRADE) {
          // during downgrades, skip the last schema
          break;
        }
        if (current.ruleSet().hasRules(migrationMode)) {
          migrations.add(new Migration(previous, current));
        }
        previous = current;
      }
    }
    return migrations;
  }

  private List<ParsedSchema> getSchemasBetween(
      String subject, ParsedSchema schema1, ParsedSchema schema2)
      throws IOException, RestClientException {
    if (Math.abs(schema1.version() - schema2.version()) <= 1) {
      return ImmutableList.of(schema1, schema2);
    }
    int version1;
    int version2;
    boolean isDescending = false;
    if (schema1.version() < schema2.version()) {
      version1 = schema1.version();
      version2 = schema2.version();
    } else {
      version1 = schema2.version();
      version2 = schema1.version();
      isDescending = true;
    }
    List<ParsedSchema> schemas = new ArrayList<>();
    for (int i = version1 + 1; i < version2 - 1; i++) {
      schemas.add(getSchemaMetadata(subject, i));
    }
    if (isDescending) {
      Collections.reverse(schemas);
    }
    List<ParsedSchema> result = new ArrayList<>();
    result.add(schema1);
    result.addAll(schemas);
    result.add(schema2);
    return result;
  }

  public void registerMessageTransform(String name, MessageTransform transform) {
    messageTransforms.put(name, transform);
  }

  public void registerFieldTransformProvider(String name, FieldTransformProvider provider) {
    fieldTransformProviders.put(name, provider);
  }

  /**
   * Get the subject name for the given topic and value type.
   */
  protected String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    String subject;
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      subject = ((SubjectNameStrategy) subjectNameStrategy).subjectName(topic, isKey, schema);
    } else {
      subject = ((io.confluent.kafka.serializers.subject.SubjectNameStrategy) subjectNameStrategy)
          .getSubjectName(topic, isKey, value);
    }
    return getContextName(topic, subject);
  }

  protected String getContextName(String topic) {
    return getContextName(topic, null);
  }

  // Visible for testing
  protected String getContextName(String topic, String subject) {
    String contextName = contextNameStrategy.contextName(topic);
    if (contextName != null) {
      contextName = QualifiedSubject.normalizeContext(contextName);
      return subject != null ? contextName + subject : contextName;
    } else {
      return subject;
    }
  }

  protected boolean strategyUsesSchema(boolean isKey) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      return ((SubjectNameStrategy) subjectNameStrategy).usesSchema();
    } else {
      return false;
    }
  }

  protected boolean isDeprecatedSubjectNameStrategy(boolean isKey) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    return !(
        subjectNameStrategy
            instanceof io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy);
  }

  private Object subjectNameStrategy(boolean isKey) {
    return isKey ? keySubjectNameStrategy : valueSubjectNameStrategy;
  }

  /**
   * Get the subject name used by the old Encoder interface, which relies only on the value type
   * rather than the topic.
   */
  protected String getOldSubjectName(Object value) {
    if (value instanceof GenericContainer) {
      return ((GenericContainer) value).getSchema().getName() + "-value";
    } else {
      throw new SerializationException("Primitive types are not supported yet");
    }
  }

  @Deprecated
  public int register(String subject, Schema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema, normalize);
  }

  @Deprecated
  public Schema getById(int id) throws IOException, RestClientException {
    return schemaRegistry.getById(id);
  }

  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    return schemaRegistry.getSchemaById(id);
  }

  @Deprecated
  public Schema getBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getBySubjectAndId(subject, id);
  }

  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getSchemaBySubjectAndId(subject, id);
  }

  protected ParsedSchema lookupSchemaBySubjectAndId(
      String subject, int id, ParsedSchema schema, boolean idCompatStrict)
      throws IOException, RestClientException {
    ParsedSchema lookupSchema = getSchemaBySubjectAndId(subject, id);
    if (idCompatStrict && !lookupSchema.isBackwardCompatible(schema).isEmpty()) {
      throw new IOException("Incompatible schema " + lookupSchema.canonicalString()
          + " with refs " + lookupSchema.references()
          + " of type " + lookupSchema.schemaType()
          + " for schema " + schema.canonicalString()
          + ". Set id.compatibility.strict=false to disable this check");
    }
    return lookupSchema;
  }

  protected ParsedSchema lookupLatestVersion(
      String subject, ParsedSchema schema, boolean latestCompatStrict)
      throws IOException, RestClientException {
    return lookupLatestVersion(schemaRegistry, subject, schema, latestVersions, latestCompatStrict);
  }

  protected static ParsedSchema lookupLatestVersion(
      SchemaRegistryClient schemaRegistry,
      String subject,
      ParsedSchema schema,
      Map<SubjectSchema, ParsedSchema> cache,
      boolean latestCompatStrict)
      throws IOException, RestClientException {
    SubjectSchema ss = new SubjectSchema(subject, schema);
    ParsedSchema latestVersion = null;
    if (cache != null) {
      latestVersion = cache.get(ss);
    }
    if (latestVersion == null) {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      latestVersion = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      // Sanity check by testing latest is backward compatibility with schema
      // Don't test for forward compatibility so unions can be handled properly
      if (latestCompatStrict && !latestVersion.isBackwardCompatible(schema).isEmpty()) {
        throw new IOException("Incompatible schema " + schemaMetadata.getSchema()
            + " with refs " + schemaMetadata.getReferences()
            + " of type " + schemaMetadata.getSchemaType()
            + " for schema " + schema.canonicalString()
            + ". Set latest.compatibility.strict=false to disable this check");
      }
      if (cache != null) {
        cache.put(ss, latestVersion);
      }
    }
    return latestVersion;
  }

  protected ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer;
  }

  public Object fromJson(RuleContext ctx, ParsedSchema schema, JsonNode json) throws IOException {
    return schema.fromJson(json);
  }

  public JsonNode toJson(RuleContext ctx, ParsedSchema schema, Object object) throws IOException {
    return schema.toJson(object);
  }

  protected Object executeRules(
      String topic, String subject, Headers headers,
      RuleMode ruleMode, ParsedSchema oldSchema, ParsedSchema newSchema,
      boolean isFinalSchema, Object message) {
    if (message == null || newSchema == null) {
      return message;
    }
    List<Rule> rules;
    if (ruleMode == RuleMode.UPGRADE || ruleMode == RuleMode.DOWNGRADE) {
      rules = newSchema.ruleSet().getMigrationRules();
    } else {
      rules = newSchema.ruleSet().getDomainRules();
    }
    for (Rule rule : rules) {
      if (rule.getMode() == RuleMode.READWRITE) {
        if (ruleMode != RuleMode.READ && ruleMode != RuleMode.WRITE) {
          continue;
        }
      } else if (ruleMode != rule.getMode()) {
        continue;
      }
      RuleContext ctx = new RuleContext(this, oldSchema, newSchema, isFinalSchema,
          topic, subject, headers, isKey, ruleMode, rule);
      MessageTransform messageTransform = null;
      if (rule.getKind() == RuleKind.FIELD_XFORM) {
        FieldTransformProvider provider = fieldTransformProviders.get(rule.getType());
        if (provider != null) {
          FieldTransform fieldTransform = provider.newTransform(ctx);
          messageTransform = new MessageFieldTransform(fieldTransform);
        }
      } else {
        messageTransform = messageTransforms.get(rule.getType());
      }
      if (messageTransform == null) {
        return message;
      }
      try {
        message = messageTransform.transform(ctx, message);
        if (message == null) {
          throw new SerializationException("Validation failed for rule " + rule);
        }
      } catch (RuleException e) {
        throw new SerializationException("Could not execute rule " + rule, e);
      }
    }
    return message;
  }

  protected static KafkaException toKafkaException(RestClientException e, String errorMessage) {
    if (e.getErrorCode() / 100 == 4 /* Client Error */) {
      return new InvalidConfigurationException(e.getMessage());
    } else {
      return new SerializationException(errorMessage, e);
    }
  }

  protected static class SubjectSchema {
    private final String subject;
    private final ParsedSchema schema;

    public SubjectSchema(String subject, ParsedSchema schema) {
      this.subject = subject;
      this.schema = schema;
    }

    public String getSubject() {
      return subject;
    }

    public ParsedSchema getSchema() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubjectSchema that = (SubjectSchema) o;
      return subject.equals(that.subject)
          && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, schema);
    }
  }

  protected static class Migration {
    private final RuleMode ruleMode;
    private final ParsedSchema previous;
    private final ParsedSchema current;

    public Migration(ParsedSchema previous, ParsedSchema current) {
      this.previous = previous;
      this.current = current;
      if (previous != null && current != null) {
        if (previous.version() < current.version()) {
          this.ruleMode = RuleMode.UPGRADE;
        } else if (previous.version() > current.version()) {
          this.ruleMode = RuleMode.DOWNGRADE;
        } else {
          this.ruleMode = null;
        }
      } else {
        this.ruleMode = null;
      }
    }

    public RuleMode getRuleMode() {
      return ruleMode;
    }

    public ParsedSchema getPrevious() {
      return previous;
    }

    public ParsedSchema getCurrent() {
      return current;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Migration migration = (Migration) o;
      return ruleMode == migration.ruleMode
          && Objects.equals(previous, migration.previous)
          && Objects.equals(current, migration.current);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ruleMode, previous, current);
    }
  }

  static class ListPropertyParser {
    private static final char DELIM_CHAR = ',';
    private static final char QUOTE_CHAR = '\'';

    private final CsvMapper mapper;
    private final CsvSchema schema;

    public ListPropertyParser() {
      mapper = new CsvMapper()
          .enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING)
          .enable(CsvParser.Feature.WRAP_AS_ARRAY);
      schema = CsvSchema.builder()
          .setColumnSeparator(DELIM_CHAR)
          .setQuoteChar(QUOTE_CHAR)
          .setLineSeparator("")
          .build();
    }

    public List<String> parse(String str) {
      try {
        ObjectReader reader = mapper.readerFor(String[].class).with(schema);
        Iterator<String[]> iter = reader.readValues(str);
        String[] strings = iter.hasNext() ? iter.next() : new String[0];
        return Arrays.asList(strings);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not parse string " + str, e);
      }
    }

    public String asString(List<String> list) {
      try {
        String[] array = list.toArray(new String[0]);
        ObjectWriter writer = mapper.writerFor(Object[].class).with(schema);
        return writer.writeValueAsString(array);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Could not parse list " + list, e);
      }
    }
  }

  static class MapPropertyParser {
    private final ListPropertyParser parser;

    public MapPropertyParser() {
      parser = new ListPropertyParser();
    }

    public Map<String, String> parse(String str) {
      List<String> strings = parser.parse(str);
      return strings.stream()
          .collect(Collectors.toMap(
              s -> s.substring(0, s.indexOf('=')),
              s -> s.substring(s.indexOf('=') + 1))
          );
    }

    public String asString(Map<String, String> map) {
      List<String> entries = map.entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.toList());
      return parser.asString(entries);
    }
  }
}
