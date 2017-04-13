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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcShortQuery3Handler implements OperationHandler<LdbcShortQuery3PersonFriends, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery3Handler.class);

	public void executeOperation(LdbcShortQuery3PersonFriends operation, PostgresConnectionState state,
			ResultReporter resultReporter) throws DbException {
		List<LdbcShortQuery3PersonFriendsResult> RESULT = new ArrayList<LdbcShortQuery3PersonFriendsResult>();
		int results_count = 0;
		Connection conn = state.getConnection();
		CallableStatement stmt1 = null;
		try {

			stmt1 = conn.prepareCall("select * from person_view_3(?)");
			stmt1.setLong(1, operation.personId());

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
					long since = rs.getLong(4);
					LdbcShortQuery3PersonFriendsResult tmp = new LdbcShortQuery3PersonFriendsResult(personId, firstName,
							lastName, since);
					RESULT.add(tmp);
				}
			}
			stmt1.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("Err: LdbcShortQuery3 (" + operation.personId() + ")", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("Err: LdbcShortQuery3 (" + operation.personId() + ")", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}