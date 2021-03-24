package io.confluent.kafka.schemaregistry.json.schema;

public class FalseSchema extends UntypedSchema {

    public static final FalseSchema INSTANCE = new FalseSchema();

    public FalseSchema() {
    }

    @Override
    public String toString() {
        return "false";
    }
}
