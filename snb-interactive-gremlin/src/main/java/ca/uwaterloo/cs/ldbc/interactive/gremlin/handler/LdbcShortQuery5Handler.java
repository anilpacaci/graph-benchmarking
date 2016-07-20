package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import net.ellitron.ldbcsnbimpls.interactive.core.Entity;
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
public class LdbcShortQuery5Handler implements OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator ldbcShortQuery5MessageCreator, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("message_id", GremlinUtils.makeIid(Entity.MESSAGE, ldbcShortQuery5MessageCreator.messageId()));

        List<Result> results = null;
        try {
            results = client.submit("g.V().has('iid', message_id).outE('hasCreator').inV()", params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        Vertex creator = results.get(0).getVertex();

        LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(GremlinUtils.getSNBId(creator), creator.<String>property("firstName").value(), creator.<String>property("lastName").value());

        resultReporter.report(1, result, ldbcShortQuery5MessageCreator);

    }
}
