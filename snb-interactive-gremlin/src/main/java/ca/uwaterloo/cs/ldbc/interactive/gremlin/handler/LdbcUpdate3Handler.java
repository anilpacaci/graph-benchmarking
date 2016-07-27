package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import org.apache.tinkerpop.gremlin.driver.Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by anilpacaci on 2016-07-21.
 */
public class LdbcUpdate3Handler implements OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike ldbcUpdate3AddCommentLike, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate3AddCommentLike.personId()));
        params.put("comment_id", GremlinUtils.makeIid(Entity.POST, ldbcUpdate3AddCommentLike.commentId()));

        List<Object> likesProperties = new ArrayList<>();
        likesProperties.add("creationDate");
        likesProperties.add(String.valueOf(ldbcUpdate3AddCommentLike.creationDate()));
        params.put("likesProperties", likesProperties.toArray());

        try {
            client.submit("person = g.V().has('iid', person_id).next(); " +
                "comment = g.V().has('iid', comment_id).next(); " +
                "person.addEdge('likes', comment, likesProperties)", params).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new DbException("Remote execution failed", e);
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate3AddCommentLike);

    }
}
