package io.confluent.kafka.schemaregistry.json.schema;

import java.util.Objects;

public class NumberSchema extends Schema {

    private boolean requiresNumber;

    private Number minimum;

    private Number maximum;

    private Number multipleOf;

    private boolean exclusiveMinimum;

    private boolean exclusiveMaximum;

    private Number exclusiveMinimumLimit;

    private Number exclusiveMaximumLimit;

    private boolean requiresInteger;

    public NumberSchema() {
        super();
    }

    public Number getMaximum() {
        return maximum;
    }

    public Number getMinimum() {
        return minimum;
    }

    public Number getMultipleOf() {
        return multipleOf;
    }

    public boolean isExclusiveMaximum() {
        return exclusiveMaximum;
    }

    public boolean isExclusiveMinimum() {
        return exclusiveMinimum;
    }

    public boolean requiresInteger() {
        return requiresInteger;
    }

    public boolean isRequiresNumber() {
        return requiresNumber;
    }

    public Number getExclusiveMinimumLimit() {
        return exclusiveMinimumLimit;
    }

    public Number getExclusiveMaximumLimit() {
        return exclusiveMaximumLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof NumberSchema) {
            NumberSchema that = (NumberSchema) o;
            return that.canEqual(this) &&
                    requiresNumber == that.requiresNumber &&
                    exclusiveMinimum == that.exclusiveMinimum &&
                    exclusiveMaximum == that.exclusiveMaximum &&
                    Objects.equals(exclusiveMinimumLimit, that.exclusiveMinimumLimit) &&
                    Objects.equals(exclusiveMaximumLimit, that.exclusiveMaximumLimit) &&
                    requiresInteger == that.requiresInteger &&
                    Objects.equals(minimum, that.minimum) &&
                    Objects.equals(maximum, that.maximum) &&
                    Objects.equals(multipleOf, that.multipleOf) &&
                    super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(super.hashCode(), requiresNumber, minimum, maximum, multipleOf, exclusiveMinimum, exclusiveMaximum,
                        exclusiveMinimumLimit, exclusiveMaximumLimit, requiresInteger);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof NumberSchema;
    }
}
