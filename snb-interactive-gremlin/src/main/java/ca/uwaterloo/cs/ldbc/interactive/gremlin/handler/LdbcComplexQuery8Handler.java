package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by anilpacaci on 2016-07-23.
 */
public class LdbcComplexQuery8Handler implements OperationHandler<LdbcQuery8, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery8 ldbcQuery8, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery8.personId()));
        params.put("person_label", Entity.PERSON.getName());
        params.put("result_limit", ldbcQuery8.limit());

        String statement = "g.V().has(person_label, 'iid', person_id)" +
                ".in('hasCreator').in('replyOf')" +
                ".order().by('creationDate', decr)" +
                ".by('iid_long', incr).as('comment')" +
                ".out('hasCreator').as('person')" +
                ".limit(result_limit)" +
                ".select('person', 'comment');";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }


        List<LdbcQuery8Result> resultList = new ArrayList<>();
        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Vertex person = (Vertex) map.get("person");
            Vertex comment = (Vertex) map.get("comment");

            LdbcQuery8Result ldbcQuery8Result = new LdbcQuery8Result(GremlinUtils.getSNBId(person),
                    person.<String>property("firstName").value(),
                    person.<String>property("lastName").value(),
                    comment.<Long>property("creationDate").value(),
                    GremlinUtils.getSNBId(comment),
                    comment.<String>property("content").value());

            resultList.add(ldbcQuery8Result);
        }
        resultReporter.report(resultList.size(), resultList, ldbcQuery8);
    }
}
