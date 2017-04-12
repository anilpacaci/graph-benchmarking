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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcShortQuery7Handler
		implements OperationHandler<LdbcShortQuery7MessageReplies, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery7Handler.class);

	public void executeOperation(LdbcShortQuery7MessageReplies operation, PostgresConnectionState state,
			ResultReporter resultReporter) throws DbException {
		List<LdbcShortQuery7MessageRepliesResult> RESULT = new ArrayList<LdbcShortQuery7MessageRepliesResult>();
		int results_count = 0;
		Connection conn = state.getConnection();
		CallableStatement stmt1 = null;
		try {
			stmt1 = conn.prepareCall("select * from post_view_4(?)");
			stmt1.setLong(1, operation.messageId());

			boolean results = stmt1.execute();
			if (results) {
				ResultSet rs = stmt1.getResultSet();
				while (rs.next()) {
					results_count++;
					long commentId;
					commentId = rs.getLong(1);
					String commentContent = new String(rs.getString(2).getBytes("UTF-8"));
					;
					long creationDate = rs.getLong(3);
					long personId;
					personId = rs.getLong(4);
					String firstName = new String(rs.getString(5).getBytes("UTF-8"));
					;
					String lastName = new String(rs.getString(6).getBytes("UTF-8"));
					;
					boolean knows = rs.getBoolean(7);
					LdbcShortQuery7MessageRepliesResult tmp = new LdbcShortQuery7MessageRepliesResult(commentId,
							commentContent, creationDate, personId, firstName, lastName, knows);
					RESULT.add(tmp);
				}
			}
			stmt1.close();
			conn.close();

		} catch (SQLException e) {
			logger.error("Err: LdbcShortQuery7 (" + operation.messageId() + ")", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("Err: LdbcShortQuery7 (" + operation.messageId() + ")", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}