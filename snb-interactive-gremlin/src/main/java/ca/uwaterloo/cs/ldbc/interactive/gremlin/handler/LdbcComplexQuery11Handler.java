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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by anilpacaci on 2016-07-23.
 */
public class LdbcComplexQuery11Handler implements OperationHandler<LdbcQuery11, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery11 ldbcQuery11, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery11.personId()));
        params.put("country_name", ldbcQuery11.countryName());
        params.put("start_year", Integer.toString(ldbcQuery11.workFromYear()));
        params.put("result_limit", ldbcQuery11.limit());

        String statement = "g.V().has('iid', person_id)" +
                ".repeat(out('knows').simplePath()).until(loops().is(gte(2))).dedup().as('friend')" +
                ".outE('workAt').has('workFrom', lte(start_year)).as('startDate').inV().as('organization')" +
                ".out('isLocatedIn').has('name', country_name)" +
                ".select('friend', 'startDate', 'organization')";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }


        List<LdbcQuery11Result> resultList = new ArrayList<>();
        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Vertex person = (Vertex) map.get("friend");
            Vertex organization = (Vertex) map.get("organization");
            Edge workAt = (Edge) map.get("startDate");

            LdbcQuery11Result ldbcQuery11Result = new LdbcQuery11Result(GremlinUtils.getSNBId(person),
                    person.<String>property("firstName").value(),
                    person.<String>property("lastName").value(),
                    organization.<String>property("name").value(),
                    Integer.parseInt(workAt.<String>property("workFrom").value()));

            resultList.add(ldbcQuery11Result);
        }

        resultList.sort(new Comparator<LdbcQuery11Result>() {
            @Override
            public int compare(LdbcQuery11Result o1, LdbcQuery11Result o2) {
                if (o1.organizationWorkFromYear() == o2.organizationWorkFromYear()) {
                    if (o1.personId() == o2.personId())
                        return o2.organizationName().compareTo(o1.organizationName());
                    else
                        return Long.compare(o1.personId(), o2.personId());
                } else
                    return Integer.compare(o1.organizationWorkFromYear(), o2.organizationWorkFromYear());
            }
        });

        if(resultList.size() > ldbcQuery11.limit()) {
            resultList = resultList.subList(0, ldbcQuery11.limit());
        }

        resultReporter.report(resultList.size(), resultList, ldbcQuery11);

    }
}
