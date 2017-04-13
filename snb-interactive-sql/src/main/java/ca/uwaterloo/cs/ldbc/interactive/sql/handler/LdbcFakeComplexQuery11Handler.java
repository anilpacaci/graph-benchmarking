package ca.uwaterloo.cs.ldbc.interactive.sql.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcFakeComplexQuery11Handler implements OperationHandler<LdbcQuery11, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcFakeComplexQuery11Handler.class);

	public void executeOperation(LdbcQuery11 operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		List<LdbcQuery11Result> RESULT = new ArrayList<LdbcQuery11Result>();
		int results_count = 0;
		Connection conn = dbConnectionState.getConnection();
		PreparedStatement stmt1 = null;

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
					LdbcQuery11Result tmp = new LdbcQuery11Result(personId, firstName, lastName, "FAKEORG", 2017);
					RESULT.add(tmp);
				}
			}
			stmt1.close();
			conn.close();

		} catch (SQLException e) {
			logger.error("ComplexQuery11 could not be executed", e);
			try {
				stmt1.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("ComplexQuery11 could not be executed", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}
