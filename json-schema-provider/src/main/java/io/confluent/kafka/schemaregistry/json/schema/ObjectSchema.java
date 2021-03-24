package io.confluent.kafka.schemaregistry.json.schema;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY,
    fieldVisibility = NONE)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectSchema extends Schema {

    private Map<String, Schema> propertySchemas;

    private boolean additionalProperties;

    private Schema schemaOfAdditionalProperties;

    private Schema propertyNameSchema;

    private List<String> requiredProperties;

    private Integer minProperties;

    private Integer maxProperties;

    private Map<String, Set<String>> propertyDependencies;

    private Map<String, Schema> schemaDependencies;

    private boolean requiresObject;

    private Map<String, Schema> patternProperties;

    public ObjectSchema() {
    }

    public Integer getMaxProperties() {
        return maxProperties;
    }

    public Integer getMinProperties() {
        return minProperties;
    }

    public Map<String, Schema> getPatternProperties() {
        return patternProperties;
    }

    public Map<String, Set<String>> getPropertyDependencies() {
        return propertyDependencies;
    }

    public Map<String, Schema> getPropertySchemas() {
        return propertySchemas;
    }

    public List<String> getRequiredProperties() {
        return requiredProperties;
    }

    public Map<String, Schema> getSchemaDependencies() {
        return schemaDependencies;
    }

    public Schema getSchemaOfAdditionalProperties() {
        return schemaOfAdditionalProperties;
    }

    public Schema getPropertyNameSchema() {
        return propertyNameSchema;
    }

    public boolean permitsAdditionalProperties() {
        return additionalProperties;
    }

    public boolean requiresObject() {
        return requiresObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ObjectSchema) {
            ObjectSchema that = (ObjectSchema) o;
            return that.canEqual(this) &&
                    additionalProperties == that.additionalProperties &&
                    requiresObject == that.requiresObject &&
                    Objects.equals(propertySchemas, that.propertySchemas) &&
                    Objects.equals(schemaOfAdditionalProperties, that.schemaOfAdditionalProperties) &&
                    Objects.equals(requiredProperties, that.requiredProperties) &&
                    Objects.equals(minProperties, that.minProperties) &&
                    Objects.equals(maxProperties, that.maxProperties) &&
                    Objects.equals(propertyDependencies, that.propertyDependencies) &&
                    Objects.equals(schemaDependencies, that.schemaDependencies) &&
                    Objects.equals(patternProperties, that.patternProperties) &&
                    Objects.equals(propertyNameSchema, that.propertyNameSchema) &&
                    super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), propertySchemas, propertyNameSchema, additionalProperties, schemaOfAdditionalProperties,
                requiredProperties,
                minProperties, maxProperties, propertyDependencies, schemaDependencies, requiresObject, patternProperties);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof ObjectSchema;
    }
}
