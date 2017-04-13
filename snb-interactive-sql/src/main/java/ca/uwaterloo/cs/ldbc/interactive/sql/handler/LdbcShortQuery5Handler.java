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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcShortQuery5Handler
		implements OperationHandler<LdbcShortQuery5MessageCreator, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery5Handler.class);

	public void executeOperation(LdbcShortQuery5MessageCreator operation, PostgresConnectionState state,
			ResultReporter resultReporter) throws DbException {
		LdbcShortQuery5MessageCreatorResult RESULT = null;
		int results_count = 0;
		Connection conn = state.getConnection();
		CallableStatement stmt1 = null;
		try {
			stmt1 = conn.prepareCall("select * from post_view_2(?)");
			stmt1.setLong(1, operation.messageId());

			boolean results = stmt1.execute();
			if (results) {
				ResultSet rs = stmt1.getResultSet();
				while (rs.next()) {
					results_count++;
					long personId;
					personId = rs.getLong(1);
					String firstName = new String(rs.getString(2).getBytes("UTF-8"));
					;
					String lastName = new String(rs.getString(3).getBytes("UTF-8"));
					;
					RESULT = new LdbcShortQuery5MessageCreatorResult(personId, firstName, lastName);
				}
			}
			stmt1.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("Err: LdbcShortQuery5 (" + operation.messageId() + ")", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("Err: LdbcShortQuery5 (" + operation.messageId() + ")", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}