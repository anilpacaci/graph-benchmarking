package ca.uwaterloo.cs.kafka.ca.uwaterloo.cs.kafka.updates;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class UpdateDeserializer implements Deserializer<Update> {

  private ObjectMapper mapper = new ObjectMapper(new JsonFactory());

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Update deserialize(String topic, byte[] data) {
    try {
      return mapper.readValue(data, Update.class);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void close() {
  }
}
