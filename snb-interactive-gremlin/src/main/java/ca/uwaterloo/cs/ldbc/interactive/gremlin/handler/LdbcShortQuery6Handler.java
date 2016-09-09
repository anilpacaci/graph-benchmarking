package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/20/16.
 */
public class LdbcShortQuery6Handler implements OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery6MessageForum ldbcShortQuery6MessageForum, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("label1", Entity.POST.getName());
        params.put("label2", Entity.COMMENT.getName());
        params.put("post_id", GremlinUtils.makeIid(Entity.POST, ldbcShortQuery6MessageForum.messageId()));
        params.put("comment_id", GremlinUtils.makeIid(Entity.COMMENT, ldbcShortQuery6MessageForum.messageId()));

        String statement = " t = g.V().has(label1, 'iid', post_id); if(!t.clone().hasNext()) t = g.V().has(label2, 'iid', comment_id);" +
                "t.until(hasLabel('post')).repeat(out('replyOf')).in('containerOf').as('forum')" +
                ".out('hasModerator').as('moderator')" +
                ".select('forum', 'moderator')";


        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        HashMap resultMap = results.get(0).get(HashMap.class);
        Vertex forum = (Vertex) resultMap.get("forum");
        Vertex moderator = (Vertex) resultMap.get("moderator");

        LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(GremlinUtils.getSNBId(forum),
                forum.<String>property("title").value(),
                GremlinUtils.getSNBId(moderator),
                moderator.<String>property("firstName").value(),
                moderator.<String>property("lastName").value());

        resultReporter.report(1, result, ldbcShortQuery6MessageForum);
    }
}
