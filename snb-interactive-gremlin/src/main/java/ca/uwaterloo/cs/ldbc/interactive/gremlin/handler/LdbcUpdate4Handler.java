package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcUpdate4Handler implements OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate4AddForum ldbcUpdate4AddForum, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();

        params.put("vlabel", Entity.FORUM.getName());
        params.put("forum_id", GremlinUtils.makeIid(Entity.FORUM, ldbcUpdate4AddForum.forumId()));
        params.put("forum_id_long", ldbcUpdate4AddForum.forumId());
        params.put("title", ldbcUpdate4AddForum.forumTitle());
        params.put("creation_date", ldbcUpdate4AddForum.creationDate().getTime());
        params.put("moderator_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate4AddForum.moderatorPersonId()));
        params.put("tag_ids", GremlinUtils.makeIid(Entity.TAG, ldbcUpdate4AddForum.tagIds()));

        params.put("person_label", Entity.PERSON.getName());
        params.put("tag_label", Entity.TAG.getName());

        String statement = "forum = g.addV(label, vlabel).property('iid', forum_id)" +
            ".property('title', title)" +
            ".property('iid_long', forum_id_long)" +
            ".property('creationDate', creation_date).next();" +
            "mod = g.V().has(person_label, 'iid', moderator_id).next();" +
            "forum.addEdge('hasModerator', mod);" +
            "tag_ids.forEach{t ->  tag = g.V().has(tag_label, 'iid', t).next(); forum.addEdge('hasTag', tag); }";
        try {
            client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate4AddForum);
    }
}
