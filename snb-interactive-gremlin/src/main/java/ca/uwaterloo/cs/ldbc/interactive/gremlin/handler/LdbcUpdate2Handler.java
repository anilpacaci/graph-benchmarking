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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by anilpacaci on 2016-07-21.
 */
public class LdbcUpdate2Handler implements OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

    final static Logger logger = LoggerFactory.getLogger( GremlinDbConnectionState.class );
    @Override
    public void executeOperation(LdbcUpdate2AddPostLike ldbcUpdate2AddPostLike, DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate2AddPostLike.personId()));
        params.put("post_id", GremlinUtils.makeIid(Entity.POST, ldbcUpdate2AddPostLike.postId()));
        params.put("creation_date", ldbcUpdate2AddPostLike.creationDate().getTime());

        params.put("person_label", Entity.PERSON.getName());
        params.put("post_label", Entity.POST.getName());

        String statement = "person = g.V().has(person_label, 'iid', person_id).next(); " +
                "post = g.V().has(post_label, 'iid', post_id).next(); " +
                "person.addEdge('likes', post).property('creationDate', creation_date);";
        try {
            client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate2AddPostLike);
        logger.info("update 2 run");

    }
}
