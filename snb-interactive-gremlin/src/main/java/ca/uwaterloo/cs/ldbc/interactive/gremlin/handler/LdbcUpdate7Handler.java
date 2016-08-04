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
import java.util.stream.Collectors;

public class LdbcUpdate7Handler implements OperationHandler<LdbcUpdate7AddComment, DbConnectionState>
{

    @Override
    public void executeOperation( LdbcUpdate7AddComment ldbcUpdate7AddComment, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
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
        params.put("tag_ids", ldbcUpdate7AddComment.tagIds());
        String statement = "comment = g.addVertex(props);" +
            "country = g.V().has('iid', country_id);" +
            "creator = g.V().has('iid', person_id).next();" +
            "country.hasNext() && comment.addEdge('isLocatedIn', country.next());" +
            "creator.hasNext() && comment.addEdge('hasCreator', creator.next());" +
            "tags_ids.forEach{t ->  tag = g.V().has('iid', t); tag.hasNext() && comment.addEdge('hasTag', tag); }";
        if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
            statement += "replied_comment = g.V().has('iid', reply_to_c_id);" +
                "replied_comment.hasNext() && comment.addEdge('replyOf', replied_comment.next());";
        }
        if (ldbcUpdate7AddComment.replyToPostId() != -1) {
            statement += "replied_post = g.V().has('iid', reply_to_c_id);" +
                "replied_post.hasNext() && comment.addEdge('replyOf', replied_post.next());";
        }
        String tag_statement = ldbcUpdate7AddComment.tagIds()
            .stream()
            .map(t -> String.format("tag = g.V().has('iid', %s);" +
                "tag.hasNext() && post.addEdge('hasTag', tag);", t))
            .collect(Collectors.joining("\n"));

        try {
            client.submit(statement, params).all().get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report( 0, LdbcNoResult.INSTANCE, ldbcUpdate7AddComment );

    }
}
