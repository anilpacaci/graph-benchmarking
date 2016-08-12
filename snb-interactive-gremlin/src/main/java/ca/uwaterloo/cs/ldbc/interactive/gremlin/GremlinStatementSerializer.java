package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.util.Map;

public class GremlinStatementSerializer implements Serializer<GremlinStatement>
{

    private ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, GremlinStatement data) {
        try {
            return mapper.writeValueAsBytes(data);
        }
        catch(JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {

    }
}

