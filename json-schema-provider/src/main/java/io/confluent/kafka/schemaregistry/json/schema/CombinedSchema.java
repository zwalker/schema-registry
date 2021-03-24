package io.confluent.kafka.schemaregistry.json.schema;

import java.util.Collection;
import java.util.Objects;

public class CombinedSchema extends Schema {

    public enum ValidationCriterion {
        ALL_OF,
        ANY_OF,
        ONE_OF
    }

    private Collection<Schema> subschemas;

    private ValidationCriterion criterion;

    public CombinedSchema() {
        super();
    }

    public ValidationCriterion getCriterion() {
        return criterion;
    }

    public Collection<Schema> getSubschemas() {
        return subschemas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof CombinedSchema) {
            CombinedSchema that = (CombinedSchema) o;
            return that.canEqual(this) &&
                    Objects.equals(subschemas, that.subschemas) &&
                    Objects.equals(criterion, that.criterion) &&
                    super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subschemas, criterion);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof CombinedSchema;
    }
}
