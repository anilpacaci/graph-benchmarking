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

public class LdbcUpdate7Handler implements OperationHandler<LdbcUpdate7AddComment, DbConnectionState>
{
    @Override
    public void executeOperation( LdbcUpdate7AddComment ldbcUpdate7AddComment, DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("comment_label", Entity.COMMENT.getName());
        params.put("place_label", Entity.PLACE.getName());
        params.put("person_label", Entity.PERSON.getName());
        params.put("post_label", Entity.POST.getName());
        params.put("tag_label", Entity.TAG.getName());
        params.put("country_id", GremlinUtils.makeIid( Entity.PLACE, ldbcUpdate7AddComment.countryId() ) );
        params.put("person_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate7AddComment.authorPersonId() ) );
        params.put("reply_to_c_id", GremlinUtils.makeIid( Entity.COMMENT, ldbcUpdate7AddComment.replyToCommentId() ) );
        params.put("reply_to_p_id", GremlinUtils.makeIid( Entity.POST, ldbcUpdate7AddComment.replyToPostId() ) );
        params.put("comment_id", GremlinUtils.makeIid( Entity.COMMENT, ldbcUpdate7AddComment.commentId() ) );
        params.put("comment_id_long", ldbcUpdate7AddComment.commentId());
        params.put("length", ldbcUpdate7AddComment.length() );
        params.put("content", ldbcUpdate7AddComment.content() );
        params.put("location_ip", ldbcUpdate7AddComment.locationIp() );
        params.put("browser_used", ldbcUpdate7AddComment.browserUsed());
        params.put("creation_date", ldbcUpdate7AddComment.creationDate().getTime());
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate7AddComment.tagIds()));

        String statement = "comment = g.addV(label, comment_label).property('iid', comment_id)" +
            ".property('iid_long', 'comment_id_long')" +
            ".property('length', length)" +
            ".property('content', content)" +
            ".property('locationIP', location_ip)" +
            ".property('creationDate', creation_date)" +
            ".property('browserUsed', browser_used).next();" +
            "country = g.V().has(place_label, 'iid', country_id).next();" +
            "creator = g.V().has(person_label, 'iid', person_id).next();" +
            "comment.addEdge('isLocatedIn', country);" +
            "comment.addEdge('hasCreator', creator);";
        if (ldbcUpdate7AddComment.replyToCommentId() != -1) {
            statement += "\nreplied_comment = g.V().has(comment_label, 'iid', reply_to_c_id).next();" +
                "comment.addEdge('replyOf', replied_comment);";
        }
        if (ldbcUpdate7AddComment.replyToPostId() != -1) {
            statement += "\nreplied_post = g.V().has(post_label, 'iid', reply_to_p_id).next();" +
                "comment.addEdge('replyOf', replied_post);";
        }

        statement += "tag_ids.forEach{t ->  tag = g.V().has(tag_label, 'iid', t).next(); comment.addEdge('hasTag', tag); }";

        client.submit( statement, params );

        resultReporter.report( 0, LdbcNoResult.INSTANCE, ldbcUpdate7AddComment );

    }
}
