package ca.uwaterloo.cs.ldbc.interactive.sql.handler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcComplexQuery13Handler implements OperationHandler<LdbcQuery13, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcComplexQuery13Handler.class);

	public void executeOperation(LdbcQuery13 operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		LdbcQuery13Result RESULT = null;
		int results_count = 0;
		Connection conn = dbConnectionState.getConnection();
		Statement stmt = null;

		try {
			String queryString = dbConnectionState.file2string("query13.txt");
			queryString = queryString.replaceAll("@Person1@", String.valueOf(operation.person1Id()));
			queryString = queryString.replaceAll("@Person2@", String.valueOf(operation.person2Id()));
			stmt = conn.createStatement();

			ResultSet result = stmt.executeQuery(queryString);
			while (result.next()) {
				results_count++;
				int shortestPathlength = result.getInt(1);
				RESULT = new LdbcQuery13Result(shortestPathlength);
			}
			stmt.close();
			conn.close();
		} catch (

		SQLException e) {
			logger.error("ComplexQuery13 could not be executed", e);
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e1) {
			}
		} catch (Exception e) {
			logger.error("ComplexQuery13 could not be executed", e);
		}
		resultReporter.report(results_count, RESULT, operation);
	}
}
