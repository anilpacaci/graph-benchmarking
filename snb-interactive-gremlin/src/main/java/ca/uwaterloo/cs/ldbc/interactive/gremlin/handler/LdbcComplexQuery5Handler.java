package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery5Handler implements OperationHandler<LdbcQuery5, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery5 ldbcQuery5, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
 //     • Description: Given a start Person,
 //     find the Forums which that Person’s friends and friends of friends (excluding start Person)
 //     became Members of after a given date.
 //     For each forum find the number of Posts that were created by any of these Persons.
 //     For each Forum and consider only those Persons which joined that particular Forum after the given date.
 //     • Parameters: Person.id ID
 //     date Date
 //     • Results:
 //     Forum.title String
 //     count 32-bit Integer // number of Posts made in Forum that were created by friends
 //     • Sort:
 //     1st count (descending)
 //         2nd Forum.id (ascending)
 //     • Limit: 20

        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery5.personId()));
        params.put("min_date", String.valueOf(ldbcQuery5.minDate()));

        String statement = "g.V().has('iid', person_id).repeat(out('knows')).times(2).emit()" +
            ".inE('hasMember').has('joinDate',gte(min_date)).outV().as('forum_name')" +
            ".in('hasCreator').as('post').out('hasContainer').select('post').count().where(is(gt(0))).as('cnt')" +
            ".order().by('cnt',decr)" +
            ".select('forum_name','cnt')" +
            ".by('name')" +
            ".limit(20);";
        List<Result> results;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcQuery5Result> resultList = new ArrayList<>();
        for (Result r : results) {
            HashMap map = r.get(HashMap.class);
            String forum_name = (String) map.get("forum_name");
            int count = (int) map.get("post_count");

            LdbcQuery5Result ldbcQuery5Result = new LdbcQuery5Result(
                forum_name,
                count
            );

            resultList.add(ldbcQuery5Result);
        }
        resultReporter.report(resultList.size(), resultList, ldbcQuery5);
    }
}
