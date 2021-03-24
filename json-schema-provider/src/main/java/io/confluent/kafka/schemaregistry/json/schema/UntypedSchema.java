package io.confluent.kafka.schemaregistry.json.schema;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = UntypedSchemaDeserializer.class)
public class UntypedSchema extends Schema {
}
