package io.confluent.kafka.schemaregistry.json.schema;

public class ConstSchema extends Schema {

    private Object permittedValue;

    public ConstSchema() {
        super();
    }

    public Object getPermittedValue() {
        return permittedValue;
    }
}
