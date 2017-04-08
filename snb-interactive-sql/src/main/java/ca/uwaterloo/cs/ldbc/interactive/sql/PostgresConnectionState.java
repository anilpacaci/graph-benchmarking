package ca.uwaterloo.cs.ldbc.interactive.sql;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.postgresql.ds.PGPoolingDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;

public class PostgresConnectionState extends DbConnectionState {

	final static Logger logger = LoggerFactory.getLogger(PostgresConnectionState.class);

	public PostgresConnectionState(Map<String, String> properties) throws ClassNotFoundException {
		super();
		String hostname = properties.getOrDefault("hostname", "localhost");
		String port = properties.getOrDefault("port", "5432");
		String database = properties.getOrDefault("database", "default");

		String dbUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;

		BasicDataSource connectionPool = new BasicDataSource();

		connectionPool.setDriverClassName("org.postgresql.Driver");
		connectionPool.setUrl(dbUrl);

		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			logger.error("Postgres JDBC driver could NOT be found");
			throw e;
		}
		Connection connection = null;

	}

	@Override
	public void close() throws IOException {

	}
}
