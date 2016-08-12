package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinKafkaDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.*;
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

        Client client = ((GremlinKafkaDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery4.personId()));
        params.put("startDate", String.valueOf(ldbcQuery4.startDate().getTime()));
        params.put("duration", ldbcQuery4.durationDays());
        Date start = ldbcQuery4.startDate();
        LocalDate end = LocalDateTime.from( (TemporalAccessor) start ).plusDays( ldbcQuery4.durationDays()).toLocalDate();
        params.put("start_date", start);
        params.put("end_date", end);
        params.put("result_limit", ldbcQuery4.limit());

        String statement = "g.V().has('iid', person_id).out('knows')" +
            ".in('hasCreator').as('friend_posts')" +
            ".has('creationDate',lt(start_date))" +
            ".out('hasTag').as('before_tags')" +
            ".select('friend_posts')" +
            ".has('creationDate', inside(start_date, end_date)))" +
            ".out('hasTag')" +
            ".is(without(select('before_tags')))" +
            ".groupCount().by('name')" +
            ".order().by(valueDecr)" +
            ".limit(result_limit)";
        List<Result> results;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<LdbcQuery4Result> resultList = new ArrayList<>();
        for (Result r : results) {
            AbstractMap.SimpleEntry<String, Long> entry = r.get(AbstractMap.SimpleEntry.class);
            String tagName = entry.getKey();
            int tagCount = Math.toIntExact(entry.getValue());

            resultList.add(new LdbcQuery4Result(tagName, tagCount));
        }
        resultReporter.report(resultList.size(), resultList, ldbcQuery4);
    }
}
