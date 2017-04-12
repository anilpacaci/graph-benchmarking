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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcUpdate4Handler implements OperationHandler<LdbcUpdate4AddForum, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4Handler.class);

	public void executeOperation(LdbcUpdate4AddForum operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		Connection conn = dbConnectionState.getConnection();
		PreparedStatement cs = null;
		try {
			String queryString = "select LdbcUpdate4AddForum(?, ?, ?, ?, ?)";
			cs = conn.prepareCall(queryString);
			cs.setLong(1, operation.forumId());
			cs.setString(2, new String(operation.forumTitle().getBytes("UTF-8"), "UTF-8"));
			// DateFormat df = new SimpleDateFormat("yyyy.MM.dd
			// HH:mm:ss.SSS'+00:00'");
			// df.setTimeZone(TimeZone.getTimeZone("GMT"));
			cs.setDate(3, new Date(operation.creationDate().getTime()));
			cs.setLong(4, operation.moderatorPersonId());
			Long tagIds1[] = new Long[operation.tagIds().size()];
			int i = 0;
			for (long temp : operation.tagIds()) {
				tagIds1[i++] = temp;
			}
			cs.setArray(5, conn.createArrayOf("int", tagIds1));
			cs.execute();
			cs.close();
			conn.close();
		} catch (Throwable e) {
			logger.error("LdbcUpdate4 could not be executed", e);
			try {
				cs.close();
				conn.close();
			} catch (SQLException e1) {
			}
		}

		resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
