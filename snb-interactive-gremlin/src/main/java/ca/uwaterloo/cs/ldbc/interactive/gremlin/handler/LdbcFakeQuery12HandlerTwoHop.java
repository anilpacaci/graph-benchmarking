package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
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
 * Created by anilpacaci on 2016-07-23.
 * runs 2-hop neighbourhood retrieval instead of Q12
 */
public class LdbcFakeQuery12HandlerTwoHop implements OperationHandler<LdbcQuery12, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery12 ldbcQuery12, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();

        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery12.personId()));
        params.put("person_label", Entity.PERSON.getName());

        String statement = "g.V().has(person_label, 'iid', person_id)" +
                ".out()" + // retrieve the second hop
                ".local(outE().limit(10)).as('relation')" + // make sure that varience is not hight, since average degre on UK and twitter is 35
                ".order().by(inV().values('iid_long'), incr)" +
                ".inV().as('friend')" +
                ".select('relation', 'friend')";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcQuery12Result> resultList = new ArrayList<>();
        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Edge edge = (Edge) r.get(HashMap.class).get("relation");
            Vertex friend = (Vertex) r.get(HashMap.class).get("friend");
            Vertex person = (Vertex) map.get("friend");

            LdbcQuery12Result ldbcQuery12Result = new LdbcQuery12Result(GremlinUtils.getSNBId(person),
                    "FAKE_FIRSTNAME",
                    "FAKE_LASTNAME",
                    new ArrayList<String>(), // fake tag list
                    2017); // fake count
            resultList.add(ldbcQuery12Result);
        }

        if (resultList.size() > ldbcQuery12.limit()) {
            resultList = resultList.subList(0, ldbcQuery12.limit());
        }

        resultReporter.report(resultList.size(), resultList, ldbcQuery12);
    }
}
