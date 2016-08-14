package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcUpdate8Handler implements OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship ldbcUpdate8AddFriendship, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("p1_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate8AddFriendship.person1Id()));
        params.put("p2_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate8AddFriendship.person2Id()));
        params.put( "creation_date", String.valueOf( ldbcUpdate8AddFriendship.creationDate().getTime() ) );
        try {
            client.submit("p1 = g.V().has('iid', p1_id).next(); " +
                "p2 = g.V().has('iid', p2_id).next(); " +
                "p1.addEdge('knows', p2).property('creation_date', creation_date);" +
                "p2.addEdge('knows', p1).property('creation_date', creation_date);",
                params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);

    }
}
