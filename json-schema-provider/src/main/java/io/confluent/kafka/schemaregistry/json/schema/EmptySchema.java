package io.confluent.kafka.schemaregistry.json.schema;

public class EmptySchema extends UntypedSchema {

    public static final EmptySchema INSTANCE = new EmptySchema();

    public EmptySchema() {
        super();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof EmptySchema) {
            EmptySchema that = (EmptySchema) o;
            return that.canEqual(this) && super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof EmptySchema;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
