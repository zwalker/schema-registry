package io.confluent.kafka.schemaregistry.json.schema;

import java.util.Optional;

public class ConditionalSchema extends Schema {

    private Schema ifSchema;
    private Schema thenSchema;
    private Schema elseSchema;

    public ConditionalSchema() {
        super();
    }

    public Optional<Schema> getIfSchema() {
        return Optional.ofNullable(ifSchema);
    }

    public Optional<Schema> getThenSchema() {
        return Optional.ofNullable(thenSchema);
    }

    public Optional<Schema> getElseSchema() {
        return Optional.ofNullable(elseSchema);
    }
}
