package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcUpdate7Handler implements OperationHandler<LdbcUpdate7AddComment, DbConnectionState>
{

    @Override
    public void executeOperation( LdbcUpdate7AddComment ldbcUpdate7AddComment, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("country_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.countryId() ) );
        params.put("person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.authorPersonId() ) );
        params.put("reply_to_c_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.replyToCommentId() ) );
        params.put("reply_to_p_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.replyToPostId() ) );
        params.put("comment_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.commentId() ) );
        params.put("length", ldbcUpdate7AddComment.length() );
        params.put("type", ldbcUpdate7AddComment.type() );
        params.put("content", ldbcUpdate7AddComment.content() );
        params.put("location_ip", ldbcUpdate7AddComment.locationIp() );
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate7AddComment.tagIds()));
        String statement = "comment = g.addV().property('iid', comment_id)" +
            ".property('length', length)" +
            ".property('type', type)" +
            ".property('content', content)" +
            ".property('location_ip', location_ip);" +
            "country = g.V().has('iid', country_id).next();" +
            "creator = g.V().has('iid', person_id).next();" +
            "comment.addEdge('isLocatedIn', country);" +
            "comment.addEdge('hasCreator', creator);";
        if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
            statement += "\nreplied_comment = g.V().has('iid', reply_to_c_id).next();" +
                "comment.addEdge('replyOf', replied_comment);";
        }
        if (ldbcUpdate7AddComment.replyToPostId() != -1) {
            statement += "\nreplied_post = g.V().has('iid', reply_to_c_id).next();" +
                "comment.addEdge('replyOf', replied_post);";
        }

        statement += "tags_ids.forEach{t ->  tag = g.V().has('iid', t).next(); comment.addEdge('hasTag', tag); }";

        try {
            client.submit(statement, params).all().get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report( 0, LdbcNoResult.INSTANCE, ldbcUpdate7AddComment );

    }
}
