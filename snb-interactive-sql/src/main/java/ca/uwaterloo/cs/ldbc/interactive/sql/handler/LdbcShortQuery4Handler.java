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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcShortQuery4Handler
		implements OperationHandler<LdbcShortQuery4MessageContent, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery4Handler.class);

	public void executeOperation(LdbcShortQuery4MessageContent operation, PostgresConnectionState state,
			ResultReporter resultReporter) throws DbException {
		LdbcShortQuery4MessageContentResult RESULT = null;
		int results_count = 0;
		Connection conn = state.getConnection();
		CallableStatement stmt1 = null;
		try {

			stmt1 = conn.prepareCall("select * from post_view_1(?)");
			stmt1.setLong(1, operation.messageId());

			boolean results = stmt1.execute();
			if (results) {
				ResultSet rs = stmt1.getResultSet();
				while (rs.next()) {
					results_count++;
					String messageContent = null;
					if (rs.getString(1) == null || rs.getString(1).length() == 0)
						messageContent = new String(rs.getString(2).getBytes("UTF-8"));
					else
						messageContent = new String(rs.getString(1).getBytes("UTF-8"));
					long creationDate = rs.getLong(3);
					RESULT = new LdbcShortQuery4MessageContentResult(messageContent, creationDate);
				}
			}
			stmt1.close();
			conn.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("Err: LdbcShortQuery4 (" + operation.messageId() + ")", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("Err: LdbcShortQuery4 (" + operation.messageId() + ")", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}