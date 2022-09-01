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

package io.confluent.kafka.schemaregistry.rules;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.header.Headers;

/**
 * A rule context.
 */
public class RuleContext {

  private JsonMapper jsonMapper;
  private ParsedSchema previousSchema;
  private ParsedSchema schema;
  private boolean isFinalSchema;
  private String subject;
  private String topic;
  private Headers headers;
  private boolean isKey;
  private RuleMode ruleMode;
  private Rule rule;
  private Map<String, String> customData = new ConcurrentHashMap<>();

  public RuleContext(
      JsonMapper jsonMapper,
      ParsedSchema schema,
      boolean isFinalSchema,
      String subject,
      String topic,
      Headers headers,
      boolean isKey,
      RuleMode ruleMode,
      Rule rule) {
    this(jsonMapper, null, schema, isFinalSchema, subject, topic, headers, isKey, ruleMode, rule);
  }

  public RuleContext(
      JsonMapper jsonMapper,
      ParsedSchema previousSchema,
      ParsedSchema schema,
      boolean isFinalSchema,
      String subject,
      String topic,
      Headers headers,
      boolean isKey,
      RuleMode ruleMode,
      Rule rule) {
    this.jsonMapper = jsonMapper;
    this.previousSchema = previousSchema;
    this.schema = schema;
    this.isFinalSchema = isFinalSchema;
    this.subject = subject;
    this.topic = topic;
    this.headers = headers;
    this.isKey = isKey;
    this.ruleMode = ruleMode;
    this.rule = rule;
  }

  public ParsedSchema previousSchema() {
    return previousSchema;
  }

  public ParsedSchema schema() {
    return schema;
  }

  public boolean isFinalSchema() {
    return isFinalSchema;
  }

  public String subject() {
    return subject;
  }

  public String topic() {
    return topic;
  }

  public Headers headers() {
    return headers;
  }

  public boolean isKey() {
    return isKey;
  }

  public RuleMode ruleMode() {
    return ruleMode;
  }

  public Rule rule() {
    return rule;
  }

  public Map<String, String> customData() {
    return customData;
  }

  public Object fromJson(JsonNode json) throws IOException {
    return jsonMapper.fromJson(this, schema, json);
  }

  public JsonNode toJson(Object object) throws IOException {
    ParsedSchema s = previousSchema != null ? previousSchema : schema;
    return jsonMapper.toJson(this, s, object);
  }
}
