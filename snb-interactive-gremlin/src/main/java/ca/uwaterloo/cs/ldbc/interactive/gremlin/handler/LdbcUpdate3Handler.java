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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by anilpacaci on 2016-07-21.
 */
public class LdbcUpdate3Handler implements OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike ldbcUpdate3AddCommentLike, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        KafkaProducer<String, GremlinStatement> producer = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaProducer();
        String topic = ((GremlinKafkaDbConnectionState) dbConnectionState).getKafkaTopic();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate3AddCommentLike.personId()));
        params.put("comment_id", GremlinUtils.makeIid(Entity.COMMENT, ldbcUpdate3AddCommentLike.commentId()));
        params.put("creation_date", String.valueOf(ldbcUpdate3AddCommentLike.creationDate().getTime()));

        params.put("person_label", Entity.PERSON.getName());
        params.put("comment_label", Entity.COMMENT.getName());

        String statement = "person = g.V().has(person_label, 'iid', person_id).next(); " +
                "comment = g.V().has(comment_label, 'iid', comment_id).next(); " +
                "person.addEdge('likes', comment).property('creationDate', creation_date);";

        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate3AddCommentLike);

    }
}
