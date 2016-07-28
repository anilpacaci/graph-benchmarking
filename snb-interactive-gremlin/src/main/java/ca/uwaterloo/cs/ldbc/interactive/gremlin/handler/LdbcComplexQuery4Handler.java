package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcComplexQuery4Handler implements OperationHandler<LdbcQuery4, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery4 ldbcQuery4, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
       // Description: Given a start Person, find Tags that are attached to Posts that were created by that Person’s friends.
       // Only include Tags that were attached to friends’ Posts created within a given time interval, and that were never
       // attached to friends’ Posts created before this interval.
       // • Parameters:
       // Person.id ID
       // startDate Date
       // duration 32-bit Integer
       // • Results:
       // Tag.name String
       // count 32-bit Integer
       // • Sort:
       // 1st count (descending)
       // 2nd Tag.name (ascending)
       // • Limit: 10
       // duration of requested period, in days
       // the interval [startDate, startDate + Duration) is closed-open
       // number of Posts made within the given time interval that have this Tag

        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery4.personId()));
        params.put("startDate", String.valueOf(ldbcQuery4.startDate().getTime()));
        params.put("duration", ldbcQuery4.durationDays());

        String statement = "g.V().has('iid', person_id).out('knows')" +
            ".in('hasCreator').as('friend_posts')" +
            ".where(__.creationDate.is(lt(start_date)))" +
            ".out('hasTag').as('before_tags)" +
            ".optional('friend_posts')" +
            ".where(__.creationDate.is(gte(start_date)))" +
            ".where(__.creationDate.is(lt(start_date + duration)))" +
            ".out('hasTag')" +
            ".except('before_tags')" +
            ".as('tag_names')" +
            ".groupCount().by('name')" +
            ".order(local).by(valueDecr).as('count')" +
            ".sort{a,b -> b.value <=> a.value}" +
            ".limit(local, 10)" +
            ".select('tag_names', 'count')";
        List<Result> results;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcQuery4Result> resultList = new ArrayList<>();
        for (Result r : results) {
            HashMap map = r.get(HashMap.class);
            String tagName = (String) map.get("tag_name");
            int count = (int) map.get("count");

            LdbcQuery4Result ldbcQuery4Result = new LdbcQuery4Result(
                tagName,
                count
            );

            resultList.add(ldbcQuery4Result);
        }
        resultReporter.report(resultList.size(), resultList, ldbcQuery4);
    }
}
