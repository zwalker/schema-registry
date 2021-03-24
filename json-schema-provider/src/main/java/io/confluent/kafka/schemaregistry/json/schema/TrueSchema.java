package io.confluent.kafka.schemaregistry.json.schema;

public class TrueSchema extends EmptySchema {

    public TrueSchema() {
        super();
    }


    @Override
    public String toString() {
        return "true";
    }

    @Override public boolean equals(Object o) {
        return o instanceof TrueSchema && super.equals(o);
    }
}
