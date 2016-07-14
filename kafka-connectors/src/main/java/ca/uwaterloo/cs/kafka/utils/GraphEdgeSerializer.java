package ca.uwaterloo.cs.kafka.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by apacaci on 5/16/16.
 */
public class GraphEdgeSerializer implements Serializer<GraphEdge> {

    private ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, GraphEdge data) {
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
