package ca.uwaterloo.cs.ldbc.interactive.gremlin;

import ca.uwaterloo.cs.ldbc.interactive.gremlin.handler.*;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.IOException;
import java.util.Map;

/**
 * An implementation for LDBC SNB Interactive Benchmark.
 * Queries implemented in Gremlin traversal language and issued against a Gremlin Server
 *
 *
 * @author apacaci
 */
public class GremlinDb extends Db{
    private GremlinDbConnectionState connection;

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {

        connection = new GremlinDbConnectionState(map);

        // Complex Queries


        registerOperationHandler(LdbcQuery12.class, LdbcComplexQuery12Handler.class);
        registerOperationHandler(LdbcQuery13.class, LdbcComplexQuery13Handler.class);

        // Short Queries
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1Handler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2Handler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3Handler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4Handler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5Handler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6Handler.class);

        // Update Queries

        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2Handler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3Handler.class);
    }

    @Override
    protected void onClose() throws IOException {
        connection.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connection;
    }
}
