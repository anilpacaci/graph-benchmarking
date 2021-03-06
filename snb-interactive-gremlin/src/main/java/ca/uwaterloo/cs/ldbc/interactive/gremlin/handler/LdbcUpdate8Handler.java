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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcUpdate8Handler implements OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {
    final static Logger logger = LoggerFactory.getLogger( GremlinDbConnectionState.class );
    @Override
    public void executeOperation(LdbcUpdate8AddFriendship ldbcUpdate8AddFriendship, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("p1_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate8AddFriendship.person1Id()));
        params.put("p2_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate8AddFriendship.person2Id()));
        params.put("person_label", Entity.PERSON.getName());
        params.put( "creation_date", ldbcUpdate8AddFriendship.creationDate().getTime() );
        String statement = "p1 = g.V().has(person_label, 'iid', p1_id).next(); " +
                "p2 = g.V().has(person_label, 'iid', p2_id).next(); " +
                "p1.addEdge('knows', p2).property('creationDate', creation_date);" +
                "p2.addEdge('knows', p1).property('creationDate', creation_date);";
        try {
            client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);
        logger.info("update 8 run");
    }
}
