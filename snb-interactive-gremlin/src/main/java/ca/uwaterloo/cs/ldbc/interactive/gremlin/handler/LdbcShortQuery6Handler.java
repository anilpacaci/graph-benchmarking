package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinKafkaDbConnectionState;
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
        Client client = ((GremlinKafkaDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("message_id", GremlinUtils.makeIid(Entity.MESSAGE, ldbcShortQuery6MessageForum.messageId()));

        List<Result> results = null;
        try {
            results = client.submit("g.V().has('message_id', message_id).repeat(out('replyOf')).until(hasLabel('post')).in('containerOf').as('forum').out('moderator').as('moderator').select('forum', 'moderator')", params).all().get();
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
