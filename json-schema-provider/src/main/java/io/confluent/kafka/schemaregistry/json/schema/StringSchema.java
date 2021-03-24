package io.confluent.kafka.schemaregistry.json.schema;

import static java.util.Objects.requireNonNull;
import static org.everit.json.schema.FormatValidator.NONE;

import java.util.Objects;

public class StringSchema extends Schema {

    private Integer minLength;

    private Integer maxLength;

    private String pattern;

    private boolean requiresString;

    private String format;

    public StringSchema() {
        super();
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public Integer getMinLength() {
        return minLength;
    }

    public String getPattern() {
        return pattern;
    }

    public String getFormat() {
        return format;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof StringSchema) {
            StringSchema that = (StringSchema) o;
            return that.canEqual(this) &&
                    requiresString == that.requiresString &&
                    Objects.equals(minLength, that.minLength) &&
                    Objects.equals(maxLength, that.maxLength) &&
                    Objects.equals(pattern, that.pattern) &&
                    Objects.equals(format, that.format) &&
                    super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minLength, maxLength, pattern, requiresString, format);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof StringSchema;
    }

    public boolean requireString() {
        return requiresString;
    }
}
