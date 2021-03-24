package io.confluent.kafka.schemaregistry.json.schema;

import static java.util.stream.Collectors.toList;
import static org.everit.json.schema.loader.OrgJsonUtil.toMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class EnumSchema extends Schema {

    private List<Object> possibleValues;

    public EnumSchema() {
        super();
    }

    public List<Object> getPossibleValues() {
        return possibleValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof EnumSchema) {
            EnumSchema that = (EnumSchema) o;
            return that.canEqual(this) &&
                    Objects.equals(possibleValues, that.possibleValues) &&
                    super.equals(that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), possibleValues);
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof EnumSchema;
    }

}
