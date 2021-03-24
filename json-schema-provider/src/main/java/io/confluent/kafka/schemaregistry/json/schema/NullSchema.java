package io.confluent.kafka.schemaregistry.json.schema;

public class NullSchema extends Schema {

    public static final NullSchema INSTANCE = new NullSchema();

    public NullSchema() {
        super();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof NullSchema) {
            NullSchema that = (NullSchema) o;
            return that.canEqual(this) && super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof NullSchema;
    }

}
