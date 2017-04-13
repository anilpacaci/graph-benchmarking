package ca.uwaterloo.cs.ldbc.interactive.sql.handler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class LdbcComplexQuery11Handler implements OperationHandler<LdbcQuery11, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcComplexQuery11Handler.class);

	public void executeOperation(LdbcQuery11 operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		List<LdbcQuery11Result> RESULT = new ArrayList<LdbcQuery11Result>();
		int results_count = 0;
		Connection conn = dbConnectionState.getConnection();
		Statement stmt = null;

		try {
			String queryString = dbConnectionState.file2string("query11.txt");
			queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
			queryString = queryString.replaceAll("@Date0@", String.valueOf(operation.workFromYear()));
			queryString = queryString.replaceAll("@Country@", operation.countryName());
			stmt = conn.createStatement();

			ResultSet result = stmt.executeQuery(queryString);
			while (result.next()) {
				results_count++;
				String personFirstName = new String(result.getString(1).getBytes("UTF-8"));
				String personLastName = new String(result.getString(2).getBytes("UTF-8"));
				int organizationWorkFromYear = result.getInt(3);
				String organizationName = new String(result.getString(4).getBytes("UTF-8"));
				long personId;
				personId = result.getLong(5);
				;
				LdbcQuery11Result tmp = new LdbcQuery11Result(personId, personFirstName, personLastName,
						organizationName, organizationWorkFromYear);
				RESULT.add(tmp);
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("ComplexQuery11 could not be executed", e);
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("ComplexQuery11 could not be executed", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}
