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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class LdbcUpdate4Handler implements OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {
    private KafkaProducer<String, GremlinStatement> producer;

    public LdbcUpdate4Handler() {
        producer = LdbcKafkaProducer.createProducer();
    }

    @Override
    public void executeOperation(LdbcUpdate4AddForum ldbcUpdate4AddForum, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        String topic = LdbcKafkaProducer.KAFKA_TOPIC;
        Map<String, Object> params = new HashMap<>();

        Map<String, Object> vertex_props = new HashMap<>();
        vertex_props.put("forum_id", GremlinUtils.makeIid(Entity.FORUM, ldbcUpdate4AddForum.forumId()));
        vertex_props.put("title", ldbcUpdate4AddForum.forumTitle());
        vertex_props.put("creation_date", String.valueOf(ldbcUpdate4AddForum.creationDate().getTime()));

        params.put("props", vertex_props);
        params.put("moderator_id", ldbcUpdate4AddForum.moderatorPersonId());
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate4AddForum.tagIds()));
        String statement = "forum = g.addVertex(props);" +
            "mod = g.V().has('iid', moderator_id).next();" +
            "g.outE(hasModerator, mod);" +
            "tags_ids.forEach{t ->  tag = g.V().has('iid', t).next(); forum.addEdge('hasTag', tag); }";

        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate4AddForum);
    }
}
