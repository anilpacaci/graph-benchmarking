package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery2Handler implements OperationHandler<LdbcQuery2, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery2 ldbcQuery2, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery2.personId()));
        params.put("max_date", ldbcQuery2.maxDate());

        String statement = "g.V().has('iid', person_id).outE('knows').as('person')" +
            ".in('hasCreator').as('message')" +
            ".where(creationDate.is(lte(max_date)))" +
            ".order().by('creationDate', decr)" +
            ".order().by('iid', asc)" +
            ".limit(20)" +
            ".select('person', 'message');";
        List<Result> results;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }


        List<LdbcQuery2Result> resultList = new ArrayList<>();
        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Vertex message = (Vertex) map.get("message");
            Vertex person= (Vertex) map.get("person");

            LdbcQuery2Result ldbcQuery2Result = new LdbcQuery2Result(
                Long.valueOf(person.<String>property("iid").value()),
                person.<String>property("firstName").value(),
                person.<String>property("lastName").value(),
                Long.valueOf(message.<String>property("iid").value()),
                message.property("content") == VertexProperty.empty() ?
                message.<String>property("imageFile").value() : message.<String>property("content").value(),
                Long.valueOf(message.<String>property("creationDate").value())
            );

            resultList.add(ldbcQuery2Result);
        }
        resultReporter.report(resultList.size(), resultList, ldbcQuery2);
    }
}
