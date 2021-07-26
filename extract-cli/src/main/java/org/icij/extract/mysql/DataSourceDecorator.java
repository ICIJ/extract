package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class DataSourceDecorator implements DataSource {

	private final DataSource dataSource;

	DataSourceDecorator(final DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return dataSource.getConnection();
	}

	@Override
	public Connection getConnection(final String username, final String password) throws SQLException {
		return dataSource.getConnection(username, password);
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return dataSource.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		return dataSource.unwrap(iface);
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return dataSource.getLoginTimeout();
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return dataSource.getLogWriter();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return dataSource.getParentLogger();
	}

	@Override
	public void setLoginTimeout(final int seconds) throws SQLException {
		dataSource.setLoginTimeout(seconds);
	}

	@Override
	public void setLogWriter(final PrintWriter out) throws SQLException {
		dataSource.setLogWriter(out);
	}
}
