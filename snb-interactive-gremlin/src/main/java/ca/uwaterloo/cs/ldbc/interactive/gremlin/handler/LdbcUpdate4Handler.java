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
        try
        {
            client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate4AddForum);
    }
}
