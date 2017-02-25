package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by anilpacaci on 2016-07-26.
 */
public class LdbcComplexQuery6Handler implements OperationHandler<LdbcQuery6, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery6 ldbcQuery6, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery6.personId()));
        params.put("person_label", Entity.PERSON.getName());
        params.put("tag_name", ldbcQuery6.tagName());
        params.put("result_limit", ldbcQuery6.limit());

        String statement = "g.V().has(person_label, 'iid', person_id)."+
        "repeat(out('knows').simplePath()).times(2).dedup()."+
        "in('hasCreator').hasLabel('post')."+
        "where(out('hasTag').has('name', tag_name))."+
        "out('hasTag').has('name', neq(tag_name)).groupCount().by('name')."+
        "order(local).by(keys).by(values, decr)."+
        "limit(local, result_limit)";
        /*
                g.V().has('person', 'iid', 'person:2738').
                repeat(out('knows').simplePath()).times(2).dedup().
                in('hasCreator').hasLabel('post').
                where(out('hasTag').has('name', 'James_Monroe')).
                out('hasTag').has('name', neq('James_Monroe')).groupCount().by('name').
                order(local).by(values, decr).by(keys).
                limit(local, 10)


                      2738,
      "James_Monroe",
      10
                */

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        HashMap<String, Long> resultMap = results.get(0).get(HashMap.class);
        List<LdbcQuery6Result> resultList = new ArrayList<>();
        for (HashMap.Entry<String, Long> entry : resultMap.entrySet()) {
            String tagName = entry.getKey();
            int tagCount = Math.toIntExact(entry.getValue());

            resultList.add(new LdbcQuery6Result(tagName, tagCount));
        }

        resultReporter.report(resultList.size(), resultList, ldbcQuery6);
    }
}
