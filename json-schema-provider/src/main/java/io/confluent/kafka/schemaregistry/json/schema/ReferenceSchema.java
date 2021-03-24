package io.confluent.kafka.schemaregistry.json.schema;

import java.util.Map;
import java.util.Objects;

public class ReferenceSchema extends Schema {

    private Schema referredSchema;

    private String refValue;

    private Map<String, Object> unprocessedProperties;

    private String title;

    private String description;

    private String schemaLocation;

    public ReferenceSchema() {
        super();
    }

    public Schema getReferredSchema() {
        return referredSchema;
    }

    public String getReferenceValue() {
        return refValue;
    }

    public void setReferredSchema(final Schema referredSchema) {
        if (this.referredSchema != null) {
            throw new IllegalStateException("referredSchema can be injected only once");
        }
        this.referredSchema = referredSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ReferenceSchema) {
            ReferenceSchema that = (ReferenceSchema) o;
            return that.canEqual(this) &&
                    Objects.equals(refValue, that.refValue) &&
                    Objects.equals(unprocessedProperties, that.unprocessedProperties) &&
                    Objects.equals(referredSchema, that.referredSchema) &&
                    Objects.equals(title, that.title) &&
                    Objects.equals(description, that.description) &&
                    super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), referredSchema, refValue, unprocessedProperties, title, description);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof ReferenceSchema;
    }

    @Override public Map<String, Object> getUnprocessedProperties() {
        return unprocessedProperties == null ? super.getUnprocessedProperties() : unprocessedProperties;
    }

    @Override public String getTitle() {
        return title == null ? super.getTitle() : title;
    }

    @Override public String getDescription() {
        return description == null ? super.getDescription() : description;
    }

    @Override public String getSchemaLocation() {
        return schemaLocation == null ? super.getSchemaLocation() : schemaLocation;
    }
}
