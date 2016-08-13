package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
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
public class LdbcShortQuery4Handler implements OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery4MessageContent ldbcShortQuery4MessageContent, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("message_id", GremlinUtils.makeIid(Entity.MESSAGE, ldbcShortQuery4MessageContent.messageId()));

        String statement = "g.V().has('iid', message_id)";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        Vertex message = results.get(0).getVertex();

        LdbcShortQuery4MessageContentResult result =
                new LdbcShortQuery4MessageContentResult(
                        message.<String>property("content").value(),
                        Long.decode(message.<String>property("creationDate").value()));

        resultReporter.report(1, result, ldbcShortQuery4MessageContent);
    }
}
