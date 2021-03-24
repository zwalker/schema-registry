package io.confluent.kafka.schemaregistry.json.schema;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;

public class UntypedSchemaDeserializer extends JsonDeserializer<UntypedSchema> {

    @Override
    public UntypedSchema deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);
        UntypedSchema schema;

        if (node.isBoolean()) {
          schema = new FalseSchema();
        } else {
          schema = new EmptySchema();
        }

        return schema;
    }
}