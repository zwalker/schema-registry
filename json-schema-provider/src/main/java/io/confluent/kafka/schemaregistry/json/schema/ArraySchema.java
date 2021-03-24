package io.confluent.kafka.schemaregistry.json.schema;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;
import java.util.Objects;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY,
    fieldVisibility = NONE)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ArraySchema extends Schema {

    private Integer minItems;

    private Integer maxItems;

    private boolean uniqueItems;

    private Schema allItemSchema;

    private boolean additionalItems;

    private List<Schema> itemSchemas;

    private boolean requiresArray;

    private Schema schemaOfAdditionalItems;

    private Schema containedItemSchema;

    public ArraySchema() {
        super();
    }

    public Schema getAllItemSchema() {
        return allItemSchema;
    }

    public List<Schema> getItemSchemas() {
        return itemSchemas;
    }

    public Integer getMaxItems() {
        return maxItems;
    }

    public Integer getMinItems() {
        return minItems;
    }

    public Schema getSchemaOfAdditionalItems() {
        return schemaOfAdditionalItems;
    }

    public Schema getContainedItemSchema() {
        return containedItemSchema;
    }

    public boolean needsUniqueItems() {
        return uniqueItems;
    }

    public boolean permitsAdditionalItems() {
        return additionalItems;
    }

    public boolean requiresArray() {
        return requiresArray;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ArraySchema) {
            ArraySchema that = (ArraySchema) o;
            return that.canEqual(this) &&
                    uniqueItems == that.uniqueItems &&
                    additionalItems == that.additionalItems &&
                    requiresArray == that.requiresArray &&
                    Objects.equals(minItems, that.minItems) &&
                    Objects.equals(maxItems, that.maxItems) &&
                    Objects.equals(allItemSchema, that.allItemSchema) &&
                    Objects.equals(itemSchemas, that.itemSchemas) &&
                    Objects.equals(schemaOfAdditionalItems, that.schemaOfAdditionalItems) &&
                    Objects.equals(containedItemSchema, that.containedItemSchema) &&
                    super.equals(o);
        } else {
            return false;
        }
    }

    @Override
    protected boolean canEqual(final Object other) {
        return other instanceof ArraySchema;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minItems, maxItems, uniqueItems, allItemSchema,
                additionalItems, itemSchemas, requiresArray, schemaOfAdditionalItems, containedItemSchema);
    }
}
