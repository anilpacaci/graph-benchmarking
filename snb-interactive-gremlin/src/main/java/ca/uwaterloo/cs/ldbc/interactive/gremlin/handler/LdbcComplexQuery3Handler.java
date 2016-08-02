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
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery3Handler implements OperationHandler<LdbcQuery3, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery3 ldbcQuery3, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery3.personId()));
        params.put("countryX", ldbcQuery3.countryXName());
        params.put("countryY", ldbcQuery3.countryYName());
        Date start = ldbcQuery3.startDate();
        String end  = Long.toString(new DateTime(start ).plusDays(ldbcQuery3.durationDays()).toDate().getTime());
        params.put("start_date", Long.toString(start.getTime()));
        params.put("end_date", end);

        String statement = "g.V().has('iid', person_id)" +
            ".repeat(out('knows')).times(2).emit().as('person')" +
            ".where(has('place', neq(countryX)).and(has('place', neq(countryY))))" +
            ".order().by('iid', incr)" +
            ".in('hasCreator')" +
            ".where(has('place', countryX)).or(has('place, countryY))))" +
            ".has('creationDate', inside(start_date, end_date))" +
            ".group().by('hasCreator')" +
            ".by(fold().match(__.as('p').unfold().has('place', countryX).count(local).as('countx')," +
            "                 __.as('p').unfold().has('place', countryY).count(local).as('county')," +
            "                )" +
            ".select('person', 'countx', 'county')" +
            ".order().by('countx', decr))" +
            ".limit(20)";

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
