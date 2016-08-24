package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcUpdate6Handler implements OperationHandler<LdbcUpdate6AddPost,DbConnectionState>
{

    @Override
    public void executeOperation( LdbcUpdate6AddPost ldbcUpdate6AddPost,
            DbConnectionState dbConnectionState, ResultReporter resultReporter ) throws DbException
    {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();

        Map<String,Object> params = new HashMap<>();
        params.put("vlabel", Entity.POST.getName());
        params.put("post_id", GremlinUtils.makeIid( Entity.POST, ldbcUpdate6AddPost.postId() ) );
        params.put("image_file", ldbcUpdate6AddPost.imageFile() );
        params.put("creation_date", String.valueOf( ldbcUpdate6AddPost.creationDate().getTime() ) );
        params.put("location_ip", ldbcUpdate6AddPost.locationIp() );
        params.put("browser_used", ldbcUpdate6AddPost.browserUsed() );
        params.put("language", ldbcUpdate6AddPost.language() );
        params.put("content", ldbcUpdate6AddPost.content() );
        params.put("length", String.valueOf( ldbcUpdate6AddPost.length() ) );

        params.put("creator_id", GremlinUtils.makeIid( Entity.PERSON, ldbcUpdate6AddPost.authorPersonId() ) );
        params.put("forum_id", GremlinUtils.makeIid( Entity.FORUM, ldbcUpdate6AddPost.forumId() ) );
        params.put("country_id", GremlinUtils.makeIid( Entity.PLACE, ldbcUpdate6AddPost.countryId() ) );

        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate6AddPost.tagIds()));

        String statement = "post = g.addV(label, vlabel).property('iid', post_id)" +
                ".property('image_file', image_file)" +
                ".property('creation_date', creation_date)" +
                ".property('location_ip', location_ip)" +
                ".property('browser_used', browser_used)" +
                ".property('language', language)" +
                ".property('content', content)" +
                ".property('length', length); " +
                "creator = g.V().has('iid', creator_id).next(); " +
                "forum = g.V().has('iid', forum_id).next(); " +
                "country = g.V().has('iid', country_id).next(); " +
                "post.addEdge(hasCreator, creator); " +
                "post.addEdge(hasContainer, forum); " +
                "post.addEdge(isLocatedIn, country);" +
                "tags_ids.forEach{t -> tag = g.V().has('iid', t).next(); post.addEdge('hasTag', tag); };";
        try {
            client.submit(statement, params).all().get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report( 0, LdbcNoResult.INSTANCE, ldbcUpdate6AddPost );

    }
}
