package ca.uwaterloo.cs.ldbc.interactive.sql;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcComplexQuery11Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcComplexQuery13Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcShortQuery1Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcShortQuery2Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcShortQuery3Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcShortQuery4Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcShortQuery5Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcShortQuery6Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcShortQuery7Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate1Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate2Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate3Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate4Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate5Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate6Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate7Handler;
import ca.uwaterloo.cs.ldbc.interactive.sql.handler.LdbcUpdate8Handler;

/**
 * An implementation for LDBC SNB Interactive Benchmark. Queries implemented in
 * Gremlin traversal language and issued against a Gremlin Server
 *
 *
 * @author apacaci
 */
public class PostgresDb extends Db {
	private PostgresConnectionState connectionState;

	final static Logger logger = LoggerFactory.getLogger(PostgresDb.class);

	@Override
	protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {

		try {
			connectionState = new PostgresConnectionState(map);
		} catch (ClassNotFoundException e) {
			logger.error("PostgreDbConnectionState cannot be created");
			throw new DbException(e);
		}

		// Complex Queries
		registerOperationHandler(LdbcQuery11.class, LdbcComplexQuery11Handler.class);
		registerOperationHandler(LdbcQuery13.class, LdbcComplexQuery13Handler.class);

		// Short Queries
		registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1Handler.class);
		registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2Handler.class);
		registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3Handler.class);
		registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4Handler.class);
		registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5Handler.class);
		registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6Handler.class);
		registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7Handler.class);
		//
		// // Update Queries
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
		connectionState.close();
	}

	@Override
	protected DbConnectionState getConnectionState() throws DbException {
		return connectionState;
	}
}
