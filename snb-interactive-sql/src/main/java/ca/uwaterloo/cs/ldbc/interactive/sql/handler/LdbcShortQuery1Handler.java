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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcShortQuery1Handler implements OperationHandler<LdbcShortQuery1PersonProfile, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery1Handler.class);

	public void executeOperation(LdbcShortQuery1PersonProfile ldbcShortQuery1PersonProfile,
			PostgresConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
		LdbcShortQuery1PersonProfileResult RESULT = null;
		int results_count = 0;
		Connection conn = dbConnectionState.getConnection();
		CallableStatement stmt1 = null;

		try {
			stmt1 = conn.prepareCall("select * from person_view_1(?)");
			stmt1.setLong(1, ldbcShortQuery1PersonProfile.personId());

			boolean results = stmt1.execute();
			if (results) {
				ResultSet rs = stmt1.getResultSet();
				while (rs.next()) {
					results_count++;
					String firstName = new String(rs.getString(1).getBytes("UTF-8"));
					String lastName = new String(rs.getString(2).getBytes("UTF-8"));
					String gender = rs.getString(3);
					long birthday = rs.getLong(4);
					long creationDate = rs.getLong(5);
					String locationIp = rs.getString(6);
					String browserUsed = rs.getString(7);
					long cityId = rs.getLong(8);
					RESULT = new LdbcShortQuery1PersonProfileResult(firstName, lastName, birthday, locationIp,
							browserUsed, cityId, gender, creationDate);
				}
			}
			stmt1.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("ShortQuery1 could not be executed", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("ShortQuery1 could not be executed", e);
		}
		resultReporter.report(results_count, RESULT, ldbcShortQuery1PersonProfile);
	}
}
