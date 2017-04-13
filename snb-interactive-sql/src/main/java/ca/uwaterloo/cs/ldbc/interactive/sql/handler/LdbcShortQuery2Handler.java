package ca.uwaterloo.cs.ldbc.interactive.sql.handler;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcShortQuery2Handler implements OperationHandler<LdbcShortQuery2PersonPosts, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery2Handler.class);

	public void executeOperation(LdbcShortQuery2PersonPosts operation, PostgresConnectionState state,
			ResultReporter resultReporter) throws DbException {
		List<LdbcShortQuery2PersonPostsResult> RESULT = new ArrayList<LdbcShortQuery2PersonPostsResult>();
		int results_count = 0;
		Connection conn = state.getConnection();
		CallableStatement stmt1 = null;
		try {
			stmt1 = conn.prepareCall("select * from person_view_2(?)");
			stmt1.setLong(1, operation.personId());

			boolean results = stmt1.execute();
			if (results) {
				ResultSet rs = stmt1.getResultSet();
				while (rs.next()) {
					results_count++;
					long postId;
					postId = rs.getLong(1);
					String postContent = rs.getString(2);
					if (postContent == null || postContent.length() == 0)
						postContent = new String(rs.getString(3).getBytes("UTF-8"));
					else
						postContent = new String(postContent.getBytes("UTF-8"));
					long postCreationTime = rs.getLong(4);
					long origPostId = 0;
					origPostId = rs.getLong(5);
					long origPersonId = 0;
					origPersonId = rs.getLong(6);
					String origFirstName = rs.getString(7);
					if (origFirstName != null)
						origFirstName = new String(origFirstName.getBytes("UTF-8"));
					String origLastName = rs.getString(8);
					if (origLastName != null)
						origLastName = new String(rs.getString(8).getBytes("UTF-8"));
					LdbcShortQuery2PersonPostsResult tmp = new LdbcShortQuery2PersonPostsResult(postId, postContent,
							postCreationTime, origPostId, origPersonId, origFirstName, origLastName);
					RESULT.add(tmp);
				}
			}
			stmt1.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("Err: LdbcShortQuery2 (" + operation.personId() + ")", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("Err: LdbcShortQuery2 (" + operation.personId() + ")", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}