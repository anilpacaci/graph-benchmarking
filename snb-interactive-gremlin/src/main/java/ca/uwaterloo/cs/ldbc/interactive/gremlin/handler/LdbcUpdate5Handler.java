package ca.uwaterloo.cs.ldbc.interactive.gremlin.handler;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.Entity;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinDbConnectionState;
import ca.uwaterloo.cs.ldbc.interactive.gremlin.GremlinUtils;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LdbcUpdate5Handler implements OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

    final static Logger logger = LoggerFactory.getLogger( GremlinDbConnectionState.class );
    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership ldbcUpdate5AddForumMembership,
            DbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        Client client = ((GremlinDbConnectionState) dbConnectionState).getClient();
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", GremlinUtils.makeIid(Entity.PERSON, ldbcUpdate5AddForumMembership.personId()));
        params.put("forum_id", GremlinUtils.makeIid(Entity.FORUM, ldbcUpdate5AddForumMembership.forumId()));
        params.put("join_date", ldbcUpdate5AddForumMembership.joinDate().getTime());

        params.put("person_label", Entity.PERSON.getName());
        params.put("forum_label", Entity.FORUM.getName());

        String statement = "person = g.V().has(person_label, 'iid', person_id).next();" +
                          "forum = g.V().has(forum_label, 'iid', forum_id).next();" +
                          "edge = forum.addEdge('hasMember', person);" +
                          "edge.property('joinDate', join_date);";
        try {
            client.submit( statement, params ).all().get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new DbException( "Remote execution failed", e );
        }

        resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate5AddForumMembership);
        logger.info("update 5 run");
    }
}
