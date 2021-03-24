package io.confluent.kafka.schemaregistry.json.schema;

public class BooleanSchema extends Schema {

    public static final BooleanSchema INSTANCE = new BooleanSchema();

    public BooleanSchema() {
        super();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof BooleanSchema) {
            BooleanSchema that = (BooleanSchema) o;
            return that.canEqual(this) && super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    protected boolean canEqual(final Object other) {
        return other instanceof BooleanSchema;
    }
}
