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

package io.confluent.kafka.schemaregistry.json;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public enum SpecificationVersion {
  DRAFT_4 {
    @Override public String idKeyword() {
      return "id";
    }

    @Override
    List<String> metaSchemaUrls() {
      return Arrays.asList(
          "http://json-schema.org/draft-04/schema",
          "https://json-schema.org/draft-04/schema"
      );
    }

  }, DRAFT_6 {
    @Override public String idKeyword() {
      return "$id";
    }

    @Override List<String> metaSchemaUrls() {
      return Arrays.asList(
          "http://json-schema.org/draft-06/schema",
          "https://json-schema.org/draft-06/schema"
      );
    }
  }, DRAFT_7 {
    @Override public String idKeyword() {
      return DRAFT_6.idKeyword();
    }

    @Override List<String> metaSchemaUrls() {
      return Arrays.asList(
          "http://json-schema.org/draft-07/schema",
          "https://json-schema.org/draft-07/schema"
      );
    }
  };

  private static final Map<String, SpecificationVersion> lookup = new HashMap<>();

  static {
    for (SpecificationVersion m : EnumSet.allOf(SpecificationVersion.class)) {
      lookup.put(m.toString(), m);
    }
  }

  public static SpecificationVersion get(String name) {
    return lookup.get(name.toLowerCase(Locale.ROOT));
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }

  static SpecificationVersion getByMetaSchemaUrl(String metaSchemaUrl) {
    return lookupByMetaSchemaUrl(metaSchemaUrl)
        .orElseThrow(() -> new IllegalArgumentException(
            format("could not determine schema version: no meta-schema is known with URL [%s]", metaSchemaUrl)
        ));
  }

  public static Optional<SpecificationVersion> lookupByMetaSchemaUrl(String metaSchemaUrl) {
    return Arrays.stream(values())
        .filter(v -> v.metaSchemaUrls().stream().anyMatch(metaSchemaUrl::startsWith))
        .findFirst();
  }


  public abstract String idKeyword();

  abstract List<String> metaSchemaUrls();

  public boolean isAtLeast(SpecificationVersion lowerInclusiveBound) {
    return this.ordinal() >= lowerInclusiveBound.ordinal();
  }
}
