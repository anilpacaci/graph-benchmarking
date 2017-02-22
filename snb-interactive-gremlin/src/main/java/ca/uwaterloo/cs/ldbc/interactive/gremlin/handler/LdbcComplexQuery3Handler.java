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
        params.put("person_label", Entity.PERSON.getName());
        params.put("countryX", ldbcQuery3.countryXName());
        params.put("countryY", ldbcQuery3.countryYName());
        Date start = ldbcQuery3.startDate();
        Long end  = new DateTime(start).plusDays(ldbcQuery3.durationDays()).toDate().getTime();
        params.put("start_date", start.getTime());
        params.put("end_date", end);
        params.put("result_limit", ldbcQuery3.limit());

        String statement = "g.V().has(person_label, 'iid', person_id)" +
            ".repeat(out('knows')).times(2).emit().as('person')" +
            ".where(out('isLocatedIn').out('isPartOf').has('name', neq(countryX)).and().out('isLocatedIn').out('isPartOf').has('name', neq(countryY)))" +
            ".in('hasCreator')" +
            ".where(out('isLocatedIn').has('name', countryX).or().out('isLocatedIn').has('name', countryY))" +
            ".has('creationDate', inside(start_date, end_date))" +
            ".group().by(out('hasCreator'))" +
            ".by(groupCount().by(out('isLocatedIn').values('name')))";

        List<Result> results;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        HashMap<Vertex, HashMap> resultMap = results.get(0).get(HashMap.class);


        List<LdbcQuery3Result> resultList = new ArrayList<>();
        for ( Map.Entry r : resultMap.entrySet()) {

            Vertex person = (Vertex) r.getKey();
            HashMap count = (HashMap) r.getValue();

            Object countxObject = count.get(ldbcQuery3.countryXName());
            Object countyObject = count.get(ldbcQuery3.countryYName());

            long countx = 0;
            long county = 0;

            if(countxObject != null) {
                countx = (long) countxObject;
            }
            if(countyObject != null) {
                county = (long) countyObject;
            }

            LdbcQuery3Result ldbcQuery3Result = new LdbcQuery3Result(
                GremlinUtils.getSNBId(person),
                person.<String>property("firstName").value(),
                person.<String>property("lastName").value(),
                countx,
                county,
                countx + county
            );

            resultList.add(ldbcQuery3Result);
        }

        Collections.sort(resultList, (o1, o2) -> {
            long o1count = o1.xCount() + o1.yCount();
            long o2count = o2.xCount() + o2.yCount();
            if (o1count < o2count) return 1;
            else if (o1count > o2count) return -1;
            else return o1.personId() < o2.personId() ? -1 : 1;
        });


        if(resultList.size() > ldbcQuery3.limit()) {
            resultReporter.report(ldbcQuery3.limit(), resultList.subList(0, ldbcQuery3.limit()), ldbcQuery3);
        } else {
            resultReporter.report(resultList.size(), resultList, ldbcQuery3);
        }
    }
}
