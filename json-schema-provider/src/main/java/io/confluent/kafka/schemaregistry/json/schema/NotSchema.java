package io.confluent.kafka.schemaregistry.json.schema;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class NotSchema extends Schema {

    private Schema mustNotMatch;

    public NotSchema() {
        super();
    }

    public Schema getMustNotMatch() {
        return mustNotMatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof NotSchema) {
            NotSchema that = (NotSchema) o;
            return that.canEqual(this) &&
                    Objects.equals(mustNotMatch, that.mustNotMatch) &&
                    super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), mustNotMatch);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof NotSchema;
    }

}
