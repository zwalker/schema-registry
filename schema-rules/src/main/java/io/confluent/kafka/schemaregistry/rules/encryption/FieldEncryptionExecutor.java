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

package io.confluent.kafka.schemaregistry.rules.encryption;

import io.confluent.encryption.common.EncryptionStack;
import io.confluent.encryption.common.crypto.Encryption;
import io.confluent.encryption.common.crypto.EncryptionBuilder;
import io.confluent.encryption.common.encoding.EncodingExecutionContext;
import io.confluent.encryption.common.encoding.configuration.EncryptionProperties;
import io.confluent.encryption.common.encoding.crypto.processor.DecryptionProcessor;
import io.confluent.encryption.common.encoding.crypto.processor.EncryptionProcessor;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.FieldTransformProvider;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldEncryptionExecutor implements FieldTransformProvider {

  public static final String TYPE = "ENCRYPT";

  private final EncryptionProcessor encryptor;
  private final DecryptionProcessor decryptor;

  public FieldEncryptionExecutor() {
    EncryptionProperties encryptionProperties = new EncryptionProperties(new HashMap<>());
    encryptor = new EncryptionProcessor(encryptionProperties);
    decryptor = new DecryptionProcessor(encryptionProperties);
  }

  public FieldTransform newTransform(RuleContext ruleContext) {
    return (ctx, message, annotations, fieldFullName, fieldName, fieldValue) ->
        execute(ctx, annotations, fieldValue, fieldName);
  }

  private Object execute(RuleContext ctx, Set<String> annotations, Object obj, String fieldName)
      throws RuleException {
    Encryption encryption = EncryptionBuilder
        .newBuilder()
        .withProperties(ctx.schema().metadata().getProperties())
        .build();

    Object result = EncodingExecutionContext.executeInContext(
        ctx.topic(),
        ctx.headers(),
        obj,
        ctx.isKey(),
        new EncryptionStack(encryption, null, null),
        (executionContext) -> execute(ctx, annotations, obj, executionContext, fieldName));
    return result;
  }

  private Object execute(RuleContext ctx, Set<String> annotations, Object obj,
      EncodingExecutionContext executionCtx, String fieldName) {
    String body = ctx.rule().getBody();
    String[] parts = body.split(",");
    if (parts.length == 0) {
      return obj;
    }
    Set<String> tags = Arrays.stream(parts)
        .filter(annotations::contains)
        .collect(Collectors.toSet());
    if (tags.isEmpty()) {
      return obj;
    }
    switch (ctx.ruleMode()) {
      case WRITE:
        return encryptor.encrypt(obj.toString(), executionCtx, fieldName);
      case READ:
        return decryptor.decrypt(obj.toString(), executionCtx, fieldName);
      default:
        throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
    }
  }
}
