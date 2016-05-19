package ca.uwaterloo.cs.kafka.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Created by apacaci on 5/16/16.
 */
public class GraphEdgeDeserializer implements Deserializer<GraphEdge> {

    private ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public GraphEdge deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, GraphEdge.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {

    }
}
