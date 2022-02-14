/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json.diff;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import net.jimblackler.jsonschemafriend.Schema;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_NARROWED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_TO_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_TO_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNIQUE_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNIQUE_ITEMS_REMOVED;

public class ArraySchemaDiff {
  static void compare(final Context ctx, final Schema original, final Schema update) {
    compareItemSchemaObject(ctx, original, update);
    compareItemSchemaArray(ctx, original, update);
    compareAdditionalItems(ctx, original, update);
    compareAttributes(ctx, original, update);
  }

  private static void compareAttributes(
      final Context ctx, final Schema original, final Schema update
  ) {
    if (!Objects.equals(original.getMaxItems(), update.getMaxItems())) {
      if (original.getMaxItems() == null && update.getMaxItems() != null) {
        ctx.addDifference("maxItems", MAX_ITEMS_ADDED);
      } else if (original.getMaxItems() != null && update.getMaxItems() == null) {
        ctx.addDifference("maxItems", MAX_ITEMS_REMOVED);
      } else if (original.getMaxItems().intValue() < update.getMaxItems().intValue()) {
        ctx.addDifference("maxItems", MAX_ITEMS_INCREASED);
      } else if (original.getMaxItems().intValue() > update.getMaxItems().intValue()) {
        ctx.addDifference("maxItems", MAX_ITEMS_DECREASED);
      }
    }
    if (!Objects.equals(original.getMinItems(), update.getMinItems())) {
      if (original.getMinItems() == null && update.getMinItems() != null) {
        ctx.addDifference("minItems", MIN_ITEMS_ADDED);
      } else if (original.getMinItems() != null && update.getMinItems() == null) {
        ctx.addDifference("minItems", MIN_ITEMS_REMOVED);
      } else if (original.getMinItems().intValue() < update.getMinItems().intValue()) {
        ctx.addDifference("minItems", MIN_ITEMS_INCREASED);
      } else if (original.getMinItems().intValue() > update.getMinItems().intValue()) {
        ctx.addDifference("minItems", MIN_ITEMS_DECREASED);
      }
    }
    if (original.isUniqueItems() != update.isUniqueItems()) {
      if (original.isUniqueItems()) {
        ctx.addDifference("uniqueItems", UNIQUE_ITEMS_REMOVED);
      } else {
        ctx.addDifference("uniqueItems", UNIQUE_ITEMS_ADDED);
      }
    }
  }

  private static void compareAdditionalItems(
      final Context ctx, final Schema original, final Schema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("additionalItems")) {
      if (original.getAdditionalItems() != null && update.getAdditionalItems() == null) {
        ctx.addDifference(ADDITIONAL_ITEMS_REMOVED);
      } else if (original.getAdditionalItems() == null && update.getAdditionalItems() != null) {
        ctx.addDifference(ADDITIONAL_ITEMS_ADDED);
      } else {
        SchemaDiff.compare(
            ctx,
            original.getAdditionalItems(),
            update.getAdditionalItems()
        );
      }
    }
  }

  private static void compareItemSchemaArray(
      final Context ctx, final Schema original, final Schema update
  ) {
    List<Schema> originalSchemas = original.getItemsTuple();
    if (originalSchemas == null) {
      originalSchemas = Collections.emptyList();
    }
    List<Schema> updateSchemas = update.getItemsTuple();
    if (updateSchemas == null) {
      updateSchemas = Collections.emptyList();
    }
    int originalSize = originalSchemas.size();
    int updateSize = updateSchemas.size();

    final Iterator<Schema> originalIterator = originalSchemas.iterator();
    final Iterator<Schema> updateIterator = updateSchemas.iterator();
    int index = 0;
    while (originalIterator.hasNext() && index < Math.min(originalSize, updateSize)) {
      try (Context.PathScope pathScope = ctx.enterPath("items/" + index)) {
        SchemaDiff.compare(ctx, originalIterator.next(), updateIterator.next());
      }
      index++;
    }
    while (originalIterator.hasNext()) {
      try (Context.PathScope pathScope = ctx.enterPath("items/" + index)) {
        Schema originalSchema = originalIterator.next();
        if (isOpenContentModel(update)) {
          // compatible
          ctx.addDifference(ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL);
        } else {
          Schema schemaFromPartial = schemaFromPartiallyOpenContentModel(update);
          if (schemaFromPartial != null) {
            final Context subctx = ctx.getSubcontext();
            SchemaDiff.compare(subctx, originalSchema, schemaFromPartial);
            ctx.addDifferences(subctx.getDifferences());
            if (subctx.isCompatible()) {
              // compatible
              ctx.addDifference(ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            } else {
              // incompatible
              ctx.addDifference(ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            }
          } else {
            if (originalSchema.getSchemaObject() == Boolean.FALSE) {
              // compatible
              ctx.addDifference(ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL);
            } else {
              // incompatible
              ctx.addDifference(ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL);
            }
          }
        }
      }
      index++;
    }
    while (updateIterator.hasNext()) {
      try (Context.PathScope pathScope = ctx.enterPath("items/" + index)) {
        Schema updateSchema = updateIterator.next();
        if (isOpenContentModel(original)) {
          if (updateSchema.getSchemaObject() == Boolean.TRUE) {
            // compatible
            ctx.addDifference(ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL);
          } else {
            // incompatible
            ctx.addDifference(ITEM_ADDED_TO_OPEN_CONTENT_MODEL);
          }
        } else {
          Schema schemaFromPartial = schemaFromPartiallyOpenContentModel(original);
          if (schemaFromPartial != null) {
            final Context subctx = ctx.getSubcontext();
            SchemaDiff.compare(subctx, schemaFromPartial, updateSchema);
            ctx.addDifferences(subctx.getDifferences());
            if (subctx.isCompatible()) {
              // compatible
              ctx.addDifference(ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            } else {
              // incompatible
              ctx.addDifference(ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            }
          } else {
            // compatible
            ctx.addDifference(ITEM_ADDED_TO_CLOSED_CONTENT_MODEL);
          }
        }
      }
      index++;
    }
  }

  private static void compareItemSchemaObject(
      final Context ctx, final Schema original, final Schema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("items")) {
      SchemaDiff.compare(ctx, original.getItems(), update.getItems());
    }
  }

  private static boolean isOpenContentModel(final Schema schema) {
    return schema.getAdditionalItems() == null;
  }

  private static Schema schemaFromPartiallyOpenContentModel(final Schema schema) {
    return schema.getAdditionalItems();
  }
}
