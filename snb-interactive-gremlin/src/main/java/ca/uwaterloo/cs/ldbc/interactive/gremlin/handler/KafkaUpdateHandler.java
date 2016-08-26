package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinStatement;
import com.ldbc.driver.DbException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class KafkaUpdateHandler implements UpdateHandler {
    private KafkaProducer<String, GremlinStatement> producer;
    private String topic;

    public KafkaUpdateHandler( KafkaProducer<String, GremlinStatement> producer, String topic ) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void submitQuery(String statement, Map<String, Object> params) throws DbException {
        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));
    }
}
