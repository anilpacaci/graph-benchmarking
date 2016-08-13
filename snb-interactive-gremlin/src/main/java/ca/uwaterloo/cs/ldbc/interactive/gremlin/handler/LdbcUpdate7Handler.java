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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class LdbcUpdate7Handler implements OperationHandler<LdbcUpdate7AddComment, DbConnectionState>
{
    private KafkaProducer<String, GremlinStatement> producer;

    public LdbcUpdate7Handler() {
        producer = LdbcKafkaProducer.createProducer();
    }
    @Override
    public void executeOperation( LdbcUpdate7AddComment ldbcUpdate7AddComment, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        String topic = LdbcKafkaProducer.KAFKA_TOPIC;
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        props.put("comment_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.commentId() ) );
        params.put("country_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.countryId() ) );
        params.put("person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.authorPersonId() ) );
        params.put("reply_to_c_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.replyToCommentId() ) );
        params.put("reply_to_p_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.replyToPostId() ) );
        props.put("length", ldbcUpdate7AddComment.length() );
        props.put("type", ldbcUpdate7AddComment.type() );
        props.put("content", ldbcUpdate7AddComment.content() );
        props.put("location_ip", ldbcUpdate7AddComment.locationIp() );
        params.put( "props", props );
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate7AddComment.tagIds()));
        String statement = "comment = g.addVertex(props);" +
            "country = g.V().has('iid', country_id).next();" +
            "creator = g.V().has('iid', person_id).next();" +
            "comment.addEdge('isLocatedIn', country);" +
            "comment.addEdge('hasCreator', creator);" +
            "tags_ids.forEach{t ->  tag = g.V().has('iid', t).next(); comment.addEdge('hasTag', tag); }";
        if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
            statement += "replied_comment = g.V().has('iid', reply_to_c_id).next();" +
                "comment.addEdge('replyOf', replied_comment);";
        }
        if (ldbcUpdate7AddComment.replyToPostId() != -1) {
            statement += "replied_post = g.V().has('iid', reply_to_c_id).next();" +
                "comment.addEdge('replyOf', replied_post);";
        }

        producer.send(new ProducerRecord<String, GremlinStatement>(topic, new GremlinStatement(statement, params)));

        resultReporter.report( 0, LdbcNoResult.INSTANCE, ldbcUpdate7AddComment );

    }
}
