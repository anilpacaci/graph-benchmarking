package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/14/16.
 */
public class LdbcShortQuery3Handler implements OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends ldbcShortQuery3PersonFriends, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();

        List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcShortQuery3PersonFriends.personId()));
        params.put("person_label", Entity.PERSON.getName());

        String statement = "g.V().has(person_label, 'iid', person_id)" +
                ".outE('knows').as('relation')" +
                ".order().by('creationDate', decr).by(inV().values('iid'), incr)" +
                ".inV().as('friend')" +
                ".select('relation', 'friend')";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        for (Result r : results) {
            HashMap map = r.get(HashMap.class);
            Edge edge = (Edge) r.get(HashMap.class).get("relation");
            Vertex friend = (Vertex) r.get(HashMap.class).get("friend");

            LdbcShortQuery3PersonFriendsResult res =
                    new LdbcShortQuery3PersonFriendsResult(
                            GremlinUtils.getSNBId(friend),
                            friend.<String>property("firstName").value(),
                            friend.<String>property("lastName").value(),
                            edge.<Long>property("creationDate").value());
            result.add(res);
        }

        resultReporter.report(result.size(), result, ldbcShortQuery3PersonFriends);
    }
}
