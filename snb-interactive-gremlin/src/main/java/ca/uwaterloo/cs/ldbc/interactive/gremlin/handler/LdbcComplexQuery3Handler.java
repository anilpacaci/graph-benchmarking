package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery3Handler implements OperationHandler<LdbcQuery3, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery3 ldbcQuery3, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery3.personId()));
        params.put("countryX", ldbcQuery3.countryXName());
        params.put("countryY", ldbcQuery3.countryYName());
        params.put("start_date", ldbcQuery3.startDate());
        params.put("duration", ldbcQuery3.durationDays());

        String statement = "g.V().has('iid', person_id).out('knows').loop(1){it.loops < 3}" +
            ".filter{it.place != countryX && it.place != countryY}" +
            ".order().by('iid', asc)" +
            ".in('hasCreator')" +
            ".filter{it.place == countryX || it.place == countryY}" +
            ".filter{it.creationDate >= start_date}" +
            ".filter{it.creationDate < start_date + duration}" +
            ".group().by('hasCreator').limit(20)" +
            ".fold().match(__.as('person')," +
            "              __.as('p').unfold().has(place, countryX).count(local).as('countx')," +
            "              __.as('p').unfold().has(place, countryX).count(local).as('county')," +
            "              )" +
            ".select('person', 'countx', 'county')" +
            ".order().by('countx', desc)";
        List<Result> results;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcQuery3Result> resultList = new ArrayList<>();
        for (Result r : results) {
            HashMap map = r.get(HashMap.class);
            Vertex person = (Vertex) map.get("person");
            int countx = (int) map.get("countx");
            int county = (int) map.get("county");

            LdbcQuery3Result ldbcQuery3Result = new LdbcQuery3Result(
                Long.valueOf(person.<String>property("iid").value()),
                person.<String>property("firstName").value(),
                person.<String>property("lastName").value(),
                countx,
                county,
                countx + county
            );

            resultList.add(ldbcQuery3Result);
        }
        resultReporter.report(resultList.size(), resultList, ldbcQuery3);
    }
}
