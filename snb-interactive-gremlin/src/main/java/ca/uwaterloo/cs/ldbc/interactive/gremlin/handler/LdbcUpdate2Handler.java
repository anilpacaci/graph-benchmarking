package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by anilpacaci on 2016-07-21.
 */
public class LdbcUpdate2Handler implements OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike ldbcUpdate2AddPostLike, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate2AddPostLike.personId()));
        params.put("post_id", GremlinUtils.makeIid(Entity.POST, ldbcUpdate2AddPostLike.postId()));
        params.put("creation_date", String.valueOf(ldbcUpdate2AddPostLike.creationDate().getTime()));

        try {
            client.submit("person = g.V().has('iid', person_id).next(); " +
                "post = g.V().has('iid', post_id).next(); " +
                "person.addEdge('likes', post).property('creationDate', creation_date);", params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate2AddPostLike);

    }
}
