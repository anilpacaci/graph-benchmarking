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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class LdbcUpdate5Handler implements OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {
    private KafkaProducer<String, GremlinStatement> producer;

    public LdbcUpdate5Handler() {
        producer = LdbcKafkaProducer.createProducer();
    }

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership ldbcUpdate5AddForumMembership,
            DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        String topic = LdbcKafkaProducer.KAFKA_TOPIC;
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate5AddForumMembership.personId()));
        params.put("forum_id", GremlinUtils.makeIid(Entity.FORUM, ldbcUpdate5AddForumMembership.forumId()));
        params.put("join_date", String.valueOf(ldbcUpdate5AddForumMembership.joinDate().getTime()));

        String statement = "person = g.V().has('iid', person_id).next();" +
                "forum = g.V().has('iid', forum_id).next();" +
                "edge = forum.addEdge('hasMember', person);" +
                "edge.property('joinDate', join_date);";
        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate5AddForumMembership);

    }
}
