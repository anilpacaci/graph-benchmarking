package ca.uwaterloo.cs.ldbc.interactive.sql.handler;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcUpdate2Handler implements OperationHandler<LdbcUpdate2AddPostLike, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcUpdate2Handler.class);

	public void executeOperation(LdbcUpdate2AddPostLike operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		Connection conn = dbConnectionState.getConnection();
		PreparedStatement cs = null;
		try {
			String queryString = "select LdbcUpdate2AddPostLike(?, ?, ?)";
			cs = conn.prepareCall(queryString);
			cs.setLong(1, operation.personId());
			cs.setLong(2, operation.postId());
			// DateFormat df = new SimpleDateFormat("yyyy.MM.dd
			// HH:mm:ss.SSS'+00:00'");
			// df.setTimeZone(TimeZone.getTimeZone("GMT"));
			cs.setDate(3, new Date(operation.creationDate().getTime()));
			cs.execute();
			cs.close();
			conn.close();
		} catch (Throwable e) {
			logger.error("LdbcUpdate2 could not be executed", e);
			try {
				cs.close();
				conn.close();
			} catch (SQLException e1) {
			}
		}

		resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
