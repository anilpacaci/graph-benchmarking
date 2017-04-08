package ca.uwaterloo.cs.ldbc.interactive.sql;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;

/**
 * An implementation for LDBC SNB Interactive Benchmark. Queries implemented in
 * Gremlin traversal language and issued against a Gremlin Server
 *
 *
 * @author apacaci
 */
public class PostgresDb extends Db {
	private PostgresConnectionState connection;

	final static Logger logger = LoggerFactory.getLogger(PostgresDb.class);
	
	@Override
	protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {

		try {
			connection = new PostgresConnectionState(map);
		} catch (ClassNotFoundException e) {
			logger.error("PostgreDbConnectionState cannot be created");
			throw new DbException(e);
		}

		// Complex Queries

		// Short Queries

		// Update Queries

	}

	@Override
	protected void onClose() throws IOException {
		connection.close();
	}

	@Override
	protected DbConnectionState getConnectionState() throws DbException {
		return connection;
	}
}
