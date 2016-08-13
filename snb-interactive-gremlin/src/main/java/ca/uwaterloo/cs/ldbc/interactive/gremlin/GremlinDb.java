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
        // TODO 1, 10 and 14 are dummy implementation.
        registerOperationHandler(LdbcQuery2.class, LdbcComplexQuery2Handler.class);
        registerOperationHandler(LdbcQuery3.class, LdbcComplexQuery3Handler.class);
        registerOperationHandler(LdbcQuery4.class, LdbcComplexQuery4Handler.class);
        registerOperationHandler(LdbcQuery5.class, LdbcComplexQuery5Handler.class);
        registerOperationHandler(LdbcQuery6.class, LdbcComplexQuery6Handler.class);
        registerOperationHandler(LdbcQuery7.class, LdbcComplexQuery7Handler.class);
        registerOperationHandler(LdbcQuery8.class, LdbcComplexQuery8Handler.class);
        registerOperationHandler(LdbcQuery9.class, LdbcComplexQuery9Handler.class);
        registerOperationHandler(LdbcQuery10.class, LdbcComplexQuery10Handler.class);
        registerOperationHandler(LdbcQuery11.class, LdbcComplexQuery11Handler.class);
        registerOperationHandler(LdbcQuery12.class, LdbcComplexQuery12Handler.class);
        registerOperationHandler(LdbcQuery13.class, LdbcComplexQuery13Handler.class);
        registerOperationHandler(LdbcQuery14.class, LdbcComplexQuery14Handler.class);

        // Short Queries
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1Handler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2Handler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3Handler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4Handler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5Handler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6Handler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7Handler.class);

        // Update Queries
        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1Handler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2Handler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3Handler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4Handler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5Handler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6Handler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7Handler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8Handler.class);
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
