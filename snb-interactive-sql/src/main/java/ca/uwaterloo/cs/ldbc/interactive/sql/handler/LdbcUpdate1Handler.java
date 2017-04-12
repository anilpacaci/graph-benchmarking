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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson.Organization;

import ca.uwaterloo.cs.ldbc.interactive.sql.PostgresConnectionState;

public class LdbcUpdate1Handler implements OperationHandler<LdbcUpdate1AddPerson, PostgresConnectionState> {

	final static Logger logger = LoggerFactory.getLogger(LdbcUpdate1Handler.class);

	public void executeOperation(LdbcUpdate1AddPerson operation, PostgresConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		Connection conn = dbConnectionState.getConnection();
		PreparedStatement cs = null;
		try {
			String queryString = "select LdbcUpdate1AddPerson(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			cs = conn.prepareStatement(queryString);
			cs.setLong(1, operation.personId());
			cs.setString(2, new String(operation.personFirstName().getBytes("UTF-8")));
			cs.setString(3, new String(operation.personLastName().getBytes("UTF-8")));
			cs.setString(4, operation.gender());
			// DateFormat df = new SimpleDateFormat("yyyy.MM.dd
			// HH:mm:ss.SSS'+00:00'");
			// df.setTimeZone(TimeZone.getTimeZone("GMT"));
			cs.setDate(5, new Date(operation.birthday().getTime()));
			cs.setDate(6, new Date(operation.creationDate().getTime()));
			cs.setString(7, operation.locationIp());
			cs.setString(8, operation.browserUsed());
			cs.setLong(9, operation.cityId());
			cs.setArray(10, conn.createArrayOf("varchar",
					operation.languages().toArray(new String[operation.languages().size()])));
			cs.setArray(11,
					conn.createArrayOf("varchar", operation.emails().toArray(new String[operation.emails().size()])));
			Long tagIds1[] = new Long[operation.tagIds().size()];
			int i = 0;
			for (long temp : operation.tagIds()) {
				tagIds1[i++] = temp;
			}
			cs.setArray(12, conn.createArrayOf("int", tagIds1));
			Long universityIds[] = new Long[operation.studyAt().size()];
			Integer universityYears[] = new Integer[operation.studyAt().size()];
			i = 0;
			for (Organization temp : operation.studyAt()) {
				universityIds[i] = temp.organizationId();
				universityYears[i++] = temp.year();
			}
			cs.setArray(13, conn.createArrayOf("int", universityIds));
			cs.setArray(14, conn.createArrayOf("int", universityYears));
			Long companyIds[] = new Long[operation.workAt().size()];
			Integer companyYears[] = new Integer[operation.workAt().size()];
			i = 0;
			for (Organization temp : operation.workAt()) {
				companyIds[i] = temp.organizationId();
				companyYears[i++] = temp.year();
			}
			cs.setArray(15, conn.createArrayOf("int", companyIds));
			cs.setArray(16, conn.createArrayOf("int", companyYears));
			cs.execute();
			cs.close();
			conn.close();
		} catch (Throwable e) {
			logger.error("LdbcUpdate1 could not be executed", e);
			try {
				cs.close();
				conn.close();
			} catch (SQLException e1) {
			}
		}

		resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
