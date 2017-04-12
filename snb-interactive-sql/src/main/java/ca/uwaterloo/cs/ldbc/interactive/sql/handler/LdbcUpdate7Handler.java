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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcUpdate7Handler implements OperationHandler<LdbcUpdate7AddComment, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcUpdate7Handler.class);

	public void executeOperation(LdbcUpdate7AddComment operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		Connection conn = dbConnectionState.getConnection();
		PreparedStatement cs = null;
		try {
			String queryString = "select LdbcUpdate7AddComment(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			cs = conn.prepareCall(queryString);
			cs.setLong(1, operation.commentId());
			// DateFormat df = new SimpleDateFormat("yyyy.MM.dd
			// HH:mm:ss.SSS'+00:00'");
			// df.setTimeZone(TimeZone.getTimeZone("GMT"));
			cs.setDate(2, new Date(operation.creationDate().getTime()));
			cs.setString(3, operation.locationIp());
			cs.setString(4, operation.browserUsed());
			cs.setString(5, new String(operation.content().getBytes("UTF-8"), "UTF-8"));
			cs.setInt(6, operation.length());
			cs.setLong(7, operation.authorPersonId());
			cs.setLong(8, operation.countryId());
			cs.setLong(9, operation.replyToPostId());
			cs.setLong(10, operation.replyToCommentId());
			Long tagIds1[] = new Long[operation.tagIds().size()];
			int i = 0;
			for (long temp : operation.tagIds()) {
				tagIds1[i++] = temp;
			}
			cs.setArray(11, conn.createArrayOf("int", tagIds1));
			cs.execute();
			cs.close();
			conn.close();
		} catch (Throwable e) {
			logger.error("LdbcUpdate7 could not be executed", e);
			try {
				cs.close();
				conn.close();
			} catch (SQLException e1) {
			}
		}

		resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
