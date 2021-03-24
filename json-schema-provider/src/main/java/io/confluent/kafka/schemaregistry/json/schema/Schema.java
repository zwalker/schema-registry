package io.confluent.kafka.schemaregistry.json.schema;

import static java.util.Collections.unmodifiableMap;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Objects;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.EXISTING_PROPERTY,
    property = "type",
    defaultImpl = UntypedSchema.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ObjectSchema.class, name = "object")
})
public abstract class Schema {

    private String type;

    private String title;

    private String description;

    private String id;

    private String schemaLocation;

    private Object defaultValue;

    private Boolean nullable;

    private Boolean readOnly;

    private Boolean writeOnly;

    private Map<String, Object> unprocessedProperties;

    protected Schema() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof Schema) {
            Schema schema = (Schema) o;
            return schema.canEqual(this) &&
                    Objects.equals(title, schema.title) &&
                    Objects.equals(defaultValue, schema.defaultValue) &&
                    Objects.equals(description, schema.description) &&
                    Objects.equals(id, schema.id) &&
                    Objects.equals(nullable, schema.nullable) &&
                    Objects.equals(readOnly, schema.readOnly) &&
                    Objects.equals(writeOnly, schema.writeOnly) &&
                    Objects.equals(unprocessedProperties, schema.unprocessedProperties);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, description, id, defaultValue, nullable, readOnly, writeOnly, unprocessedProperties);
    }

    public String getType() {
        return type;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public String getId() {
        return id;
    }

    public String getSchemaLocation() {
        return schemaLocation;
    }

    public Object getDefaultValue() {
        return this.defaultValue;
    }

    public boolean hasDefaultValue() {
        return this.defaultValue != null;
    }

    public Boolean isNullable() {
        return nullable;
    }

    public Boolean isReadOnly() {
        return readOnly;
    }

    public Boolean isWriteOnly() {
        return writeOnly;
    }

    /**
     * Returns the properties of the original schema JSON which aren't keywords of json schema
     * (therefore they weren't recognized during schema loading).
     */
    public Map<String, Object> getUnprocessedProperties() {
        return unmodifiableMap(unprocessedProperties);
    }

    /**
     * Since we add state in subclasses, but want those subclasses to be non final, this allows us to
     * have equals methods that satisfy the equals contract.
     * <p>
     * http://www.artima.com/lejava/articles/equality.html
     *
     * @param other
     *         the subject of comparison
     * @return {@code true } if {@code this} can be equal to {@code other}
     */
    protected boolean canEqual(Object other) {
        return (other instanceof Schema);
    }

    public static void main(String[] args) throws Exception {
        read("true");
        read("{ \"type\": \"object\" }");
        read("{ }");
    }

    private static Object read(String s) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Object o = mapper.readValue(s, Schema.class);
        System.out.println(" *** o " + o);
        System.out.println(" *** cls " + o.getClass().getName());
        return o;
    }
}
