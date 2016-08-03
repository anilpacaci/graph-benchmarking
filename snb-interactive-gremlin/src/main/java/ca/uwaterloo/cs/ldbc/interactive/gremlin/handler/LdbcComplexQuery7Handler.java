package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 7/20/16.
 *
 * Based on reference Titan LDBC SNB Interactive on offficial LDBC Github Page
 *  <a href="https://github.com/ldbc/ldbc_snb_implementations/blob/master/interactive/titan/titanFTM_driver/src/main/java/hpl/alp2/titan/drivers/interactive/LdbcQuery7Handler.java">ComplexQuery7 Native Titan</a>/>
 */
public class LdbcComplexQuery7Handler implements OperationHandler<LdbcQuery7, DbConnectionState>{
    @Override
    public void executeOperation(LdbcQuery7 ldbcQuery7, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcQuery7.personId()));
        params.put("result_limit", ldbcQuery7.limit());

        List<Result> authorKnowsResults = null;
        try {
            authorKnowsResults = client.submit(" g.V().has('iid', person_id).out('knows')").all().get();

        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        List<Vertex> authorKnows = new ArrayList<>();
        authorKnowsResults.forEach(res -> { authorKnows.add(res.getVertex());});

        List<Result> results = null;
        try {
            results = client.submit("g.V().has('iid', person_id)" +
                    ".in('hasCreator').as('post')" +
                    ".inE('likes').as('like')" +
                    ".outV().as('liker')" +
                    ".select('post', 'like', 'liker')").all().get();

        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        Map<Vertex, LdbcQuery7Result> qRes = new HashMap<>();

        for(Result r : results) {
            HashMap map = r.get(HashMap.class);
            Edge like = (Edge) r.get(HashMap.class).get("like");
            Vertex liker = (Vertex) r.get(HashMap.class).get("liker");
            Vertex post = (Vertex) r.get(HashMap.class).get("post");

            boolean notKnows = !authorKnows.contains(liker);

            long id = GremlinUtils.getSNBId(liker);
            String firstName = liker.<String>property("firstName").value();
            String lastName = liker.<String>property("lastName").value();
            long likeDate = Long.parseLong(like.<String>property("creationDate").value());
            long postDate = Long.parseLong(post.<String>property("creationDate").value());
            long postID = GremlinUtils.getSNBId(post);
            String content = post.<String>property("content").value();
            if (content.length() == 0) {
                content = post.<String>property("imageFile").value();
            }
            int latency = (int) ((likeDate - postDate) / 60000);

            LdbcQuery7Result res = new LdbcQuery7Result(id, firstName, lastName, likeDate, postID, content, latency, notKnows);

            if (qRes.containsKey(liker)) {
                LdbcQuery7Result other = qRes.get(liker);
                if (other.likeCreationDate() > res.likeCreationDate()) {
                    continue;
                }
                else if (other.likeCreationDate() == res.likeCreationDate() && other.commentOrPostId() < res.commentOrPostId()) {
                    continue;
                }
            }

            qRes.put(liker, res);
        }

        List<LdbcQuery7Result> result = new ArrayList<>(qRes.values());
        Collections.sort(result, new Comparator<LdbcQuery7Result>() {
            @Override
            public int compare(LdbcQuery7Result o1, LdbcQuery7Result o2) {
                if (o1.likeCreationDate() == o2.likeCreationDate())
                    return Long.compare(o1.personId(), o2.personId());
                return Long.compare(o2.likeCreationDate(), o1.likeCreationDate());
            }
        });
        if (result.size() > ldbcQuery7.limit())
            result = result.subList(0, ldbcQuery7.limit());

        resultReporter.report(result.size(), result, ldbcQuery7);
    }
}
