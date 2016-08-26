package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinKafkaDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinStatement;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
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

    @Override
    public void executeOperation(LdbcUpdate4AddForum ldbcUpdate4AddForum, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        KafkaProducer<String, GremlinStatement> producer = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaProducer();
        String topic = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaTopic();
        Map<String, Object> params = new HashMap<>();

        params.put("vlabel", Entity.FORUM.getName());
        params.put("forum_id", GremlinUtils.makeIid(Entity.FORUM, ldbcUpdate4AddForum.forumId()));
        params.put("title", ldbcUpdate4AddForum.forumTitle());
        params.put("creation_date", String.valueOf(ldbcUpdate4AddForum.creationDate().getTime()));
        params.put("moderator_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate4AddForum.moderatorPersonId()));
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate4AddForum.tagIds()));

        params.put("person_label", Entity.PERSON.getName());
        params.put("tag_label", Entity.TAG.getName());

        String statement = "forum = g.addV(label, vlabel).property('iid', forum_id)" +
            ".property('title', title)" +
            ".property('creationDate', creation_date).next();" +
            "mod = g.V().has(person_label, 'iid', moderator_id).next();" +
            "forum.addEdge('hasModerator', mod);" +
            "tag_ids.forEach{t ->  tag = g.V().has(tag_label, 'iid', t).next(); forum.addEdge('hasTag', tag); }";
        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate4AddForum);
    }
}
