package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class GremlinStatementDeserializer implements Deserializer<GremlinStatement>
{

    private ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    @Override
    public void configure( Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public GremlinStatement deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, GremlinStatement.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {

    }
}

