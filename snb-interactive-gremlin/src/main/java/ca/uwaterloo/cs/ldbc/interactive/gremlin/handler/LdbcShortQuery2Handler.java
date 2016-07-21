package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/20/16.
 */
public class LdbcShortQuery2Handler implements OperationHandler<LdbcShortQuery2PersonPosts, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery2PersonPosts ldbcShortQuery2PersonPosts, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", ldbcShortQuery2PersonPosts.personId());
        params.put("result_limit", ldbcShortQuery2PersonPosts.limit());

        List<Result> results = null;
        try {
            results = client.submit("g.V().has('iid', person_id).inE('hasCreator').outV().limit(result_limit).order().by('creationDate', decr).by('iid', decr).as('message').select('message')", params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

    }
}
