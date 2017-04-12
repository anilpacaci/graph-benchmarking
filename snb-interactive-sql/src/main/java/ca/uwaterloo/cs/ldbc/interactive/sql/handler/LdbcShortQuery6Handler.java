package ca.uwaterloo.cs.ldbc.interactive.sql.handler;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcShortQuery6Handler implements OperationHandler<LdbcShortQuery6MessageForum, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery6Handler.class);

	public void executeOperation(LdbcShortQuery6MessageForum operation, PostgresConnectionState state,
			ResultReporter resultReporter) throws DbException {
		LdbcShortQuery6MessageForumResult RESULT = null;
		int results_count = 0;
		Connection conn = state.getConnection();
		CallableStatement stmt1 = null;
		try {
			stmt1 = conn.prepareCall("select * from post_view_3(?)");
			stmt1.setLong(1, operation.messageId());

			boolean results = stmt1.execute();
			if (results) {
				ResultSet rs = stmt1.getResultSet();
				while (rs.next()) {
					results_count++;
					long forumId;
					forumId = rs.getLong(1);
					String forumTitle = new String(rs.getString(2).getBytes("UTF-8"));
					;
					long moderatorId;
					moderatorId = rs.getLong(3);
					String moderatorFirstName = new String(rs.getString(4).getBytes("UTF-8"));
					;
					String moderatorLastName = new String(rs.getString(5).getBytes("UTF-8"));
					;
					RESULT = new LdbcShortQuery6MessageForumResult(forumId, forumTitle, moderatorId, moderatorFirstName,
							moderatorLastName);
				}
			}
			stmt1.close();
			conn.close();

		} catch (SQLException e) {
			logger.error("Err: LdbcShortQuery6 (" + operation.messageId() + ")", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("Err: LdbcShortQuery6 (" + operation.messageId() + ")", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}