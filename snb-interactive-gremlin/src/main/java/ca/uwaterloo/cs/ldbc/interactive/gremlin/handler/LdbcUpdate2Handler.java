package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinStatement;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.LdbcKafkaProducer;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by anilpacaci on 2016-07-21.
 */
public class LdbcUpdate2Handler implements OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

    private KafkaProducer<String, GremlinStatement> producer;

    LdbcUpdate2Handler() {
        producer = LdbcKafkaProducer.createProducer();
    }

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike ldbcUpdate2AddPostLike, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        String topic = LdbcKafkaProducer.KAFKA_TOPIC;
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate2AddPostLike.personId()));
        params.put("post_id", GremlinUtils.makeIid(Entity.POST, ldbcUpdate2AddPostLike.postId()));

        List<Object> likesProperties = new ArrayList<>();
        likesProperties.add("creationDate");
        likesProperties.add(String.valueOf(ldbcUpdate2AddPostLike.creationDate()));
        params.put("likesProperties", likesProperties.toArray());

        String statement = "person = g.V().has('iid', person_id).next(); " +
                "post = g.V().has('iid', post_id).next(); " +
                "person.addEdge('likes', post, likesProperties);";
        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate2AddPostLike);

    }
}
