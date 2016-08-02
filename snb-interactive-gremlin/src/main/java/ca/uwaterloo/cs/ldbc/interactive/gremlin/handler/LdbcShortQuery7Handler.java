package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/20/16.
 */
public class LdbcShortQuery7Handler implements OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState>{
    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies ldbcShortQuery7MessageReplies, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("message_id", GremlinUtils.makeIid(Entity.PERSON, ldbcShortQuery7MessageReplies.messageId()));

        List<Result> authorKnowsResults = null;
        try {
            authorKnowsResults = client.submit("message = g.V().has('iid', message_id).out('hasCreator').out('knows')").all().get();

        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<Vertex> authorKnows = new ArrayList<>();
        authorKnowsResults.forEach(res -> { authorKnows.add(res.getVertex());});

        List<Result> results = null;
        try {
            results = client.submit("g.V().has('iid', message_id).in('replyOf').as('reply').out('hasCreator').as('creator').select('reply', 'creator')").all().get();

        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();

        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Vertex reply = (Vertex) r.get(HashMap.class).get("reply");
            Vertex creator = (Vertex) r.get(HashMap.class).get("creator");

            boolean knows = authorKnows.contains(creator);

            result.add(new LdbcShortQuery7MessageRepliesResult(GremlinUtils.getSNBId(reply),
                    reply.<String>property("content").value(),
                    Long.parseLong(reply.<String>property("creationDate").value()),
                    GremlinUtils.getSNBId(creator),
                    creator.<String>property("firstName").value(),
                    creator.<String>property("lastName").value(),
                    knows));
        }

        resultReporter.report(result.size(), result, ldbcShortQuery7MessageReplies);
    }
}
