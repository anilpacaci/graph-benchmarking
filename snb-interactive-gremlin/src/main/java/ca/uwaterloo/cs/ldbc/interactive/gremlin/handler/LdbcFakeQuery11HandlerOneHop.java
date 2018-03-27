package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
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
 * runs 1-hop neighbourhood retrieval instead of Q11
 */
public class LdbcFakeQuery11HandlerOneHop implements OperationHandler<LdbcQuery11, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery11 ldbcQuery11, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();

        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery11.personId()));
        params.put("person_label", Entity.PERSON.getName());

        String statement = "g.V().has(person_label, 'iid', person_id)" +
                ".outE('knows').as('relation')" +
                ".order().by('creationDate', decr).by(inV().values('iid_long'), incr)" +
                ".inV().as('friend')" +
                ".select('relation', 'friend')";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcQuery11Result> resultList = new ArrayList<>();
        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Edge edge = (Edge) r.get(HashMap.class).get("relation");
            Vertex friend = (Vertex) r.get(HashMap.class).get("friend");
            Vertex person = (Vertex) map.get("friend");

            LdbcQuery11Result ldbcQuery11Result = new LdbcQuery11Result(GremlinUtils.getSNBId(person),
                    person.<String>property("firstName").value(),
                    person.<String>property("lastName").value(),
                    "FAKEORG",
                    2017);
            resultList.add(ldbcQuery11Result);
        }

        if (resultList.size() > ldbcQuery11.limit()) {
            resultList = resultList.subList(0, ldbcQuery11.limit());
        }

        resultReporter.report(resultList.size(), resultList, ldbcQuery11);
    }
}
