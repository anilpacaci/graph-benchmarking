package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
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
public class LdbcComplexQuery12Handler implements OperationHandler<LdbcQuery12, DbConnectionState> {
    @Override
    public void executeOperation(LdbcQuery12 ldbcQuery12, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery12.personId()));
        params.put("tagclass", ldbcQuery12.tagClassName());
        params.put("result_limit", ldbcQuery12.limit());

        String statement = "g.V().has('iid', person_id)" +
                ".out('knows').limit(result_limit).as('friends')" +
                ".in('hasCreator').where(out('replyOf').hasLabel('post').out('hasTag').repeat(out('hasType')).until(has('name', tagclass))).as('messages')" +
                ".out('hasTag').values('name').as('tags')" +
                ".select('friends', 'messages', 'tags')";

        List<Result> results = null;
        try {
            results = client.submit(statement, params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        Map<Vertex, List<Vertex>> personMessageMap = new HashMap<>();
        Map<Vertex, List<String>> personTagMap = new HashMap<>();
        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Vertex person = (Vertex) map.get("friends");
            Vertex message = (Vertex) map.get("messages");
            String tag = (String) map.get("tags");

            if(!personMessageMap.containsKey(person)) {
                personMessageMap.put(person, new ArrayList<Vertex>());
            }
            personMessageMap.get(person).add(message);

            if(!personTagMap.containsKey(person)) {
                personTagMap.put(person, new ArrayList<String>());
            }
            personTagMap.get(person).add(tag);
        }

        ArrayList<LdbcQuery12Result> ldbcQuery12Results = new ArrayList<>();
        for( Map.Entry<Vertex, List<Vertex>> entry : personMessageMap.entrySet()) {
            Vertex person = entry.getKey();
            List<Vertex> messageList = entry.getValue();
            List<String> tagList = personTagMap.get(person);

            LdbcQuery12Result ldbcQuery12Result = new LdbcQuery12Result(GremlinUtils.getSNBId(person), person.<String>property("firstName").value(),
                    person.<String>property("lastName").value(),
                    tagList,
                    messageList.size());
            ldbcQuery12Results.add(ldbcQuery12Result);
        }

        resultReporter.report(ldbcQuery12Results.size(), ldbcQuery12Results, ldbcQuery12);
    }
}
