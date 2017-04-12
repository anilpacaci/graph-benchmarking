package ca.uwaterloo.cs.ldbc.interactive.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;

public class PostgresConnectionState extends DbConnectionState {

	final static Logger logger = LoggerFactory.getLogger(PostgresConnectionState.class);

	private BasicDataSource connectionPool;

	private String queryDir;

	public PostgresConnectionState(Map<String, String> properties) throws ClassNotFoundException {
		super();
		String hostname = properties.get("hostname");
		String port = properties.get("port");
		String database = properties.get("dbname");

		String user = properties.get("user");
		String password = properties.get("password");

		String dbUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;

		connectionPool = new BasicDataSource();

		connectionPool.setDriverClassName("org.postgresql.Driver");
		connectionPool.setUrl(dbUrl);

		connectionPool.setUsername(user);
		connectionPool.setPassword(password);

		queryDir = properties.get("queryDir");

		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			logger.error("Postgres JDBC driver could NOT be found");
			throw e;
		}

	}

	public void close() {
		try {
			connectionPool.close();
		} catch (SQLException e) {
			logger.error("Error closing connection pool", e);
		}
	}

	public Connection getConnection() throws DbException {
		try {
			Connection connection = connectionPool.getConnection();
			connection.setAutoCommit(true);
			return connection;
		} catch (SQLException e) {
			logger.error("Error retrieveing connection from connection pool", e);
			throw new DbException(e);
		}
	}

	public String getQueryDir() {
		return queryDir;
	}

	public String file2string(String filename) throws Exception {
		BufferedReader reader = null;
		try {
			File file = new File(getQueryDir(), filename);
			reader = new BufferedReader(new FileReader(file));
			StringBuffer sb = new StringBuffer();

			while (true) {
				String line = reader.readLine();
				if (line == null)
					break;
				else {
					sb.append(line);
					sb.append("\n");
				}
			}
			return sb.toString();
		} catch (IOException e) {
			throw new Exception("Error openening or reading file: " + filename, e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
