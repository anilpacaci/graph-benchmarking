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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcUpdate6Handler implements OperationHandler<LdbcUpdate6AddPost, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcUpdate6Handler.class);

	public void executeOperation(LdbcUpdate6AddPost operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		Connection conn = dbConnectionState.getConnection();
		PreparedStatement cs = null;
		try {
			String queryString = "select LdbcUpdate6AddPost(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			cs = conn.prepareCall(queryString);
			cs.setLong(1, operation.postId());
			cs.setString(2, new String(operation.imageFile().getBytes("UTF-8"), "UTF-8"));
			// DateFormat df = new SimpleDateFormat("yyyy.MM.dd
			// HH:mm:ss.SSS'+00:00'");
			// df.setTimeZone(TimeZone.getTimeZone("GMT"));
			cs.setDate(3, new Date(operation.creationDate().getTime()));
			cs.setString(4, operation.locationIp());
			cs.setString(5, operation.browserUsed());
			cs.setString(6, operation.language());
			cs.setString(7, new String(operation.content().getBytes("UTF-8"), "UTF-8"));
			cs.setInt(8, operation.length());
			cs.setLong(9, operation.authorPersonId());
			cs.setLong(10, operation.forumId());
			cs.setLong(11, operation.countryId());
			Long tagIds1[] = new Long[operation.tagIds().size()];
			int i = 0;
			for (long temp : operation.tagIds()) {
				tagIds1[i++] = temp;
			}
			cs.setArray(12, conn.createArrayOf("int", tagIds1));
			cs.execute();
			cs.close();
			conn.close();
		} catch (Throwable e) {
			logger.error("LdbcUpdate6 could not be executed", e);
			try {
				cs.close();
				conn.close();
			} catch (SQLException e1) {
			}
		}

		resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
