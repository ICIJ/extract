package org.icij.extract.mysql;

import org.icij.task.Options;
import org.icij.task.annotation.Option;

import javax.sql.DataSource;
import com.mysql.cj.jdbc.MysqlDataSource;

@Option(name = "mysqlUser", description = "The MySQL database user to authenticate as.", parameter = "username")
@Option(name = "mysqlPassword", description = "The MySQL user password.", parameter = "password")
@Option(name = "mysqlHostname", description = "The MySQL server hostname.", parameter = "hostname")
@Option(name = "mysqlDatabase", description = "The MySQL database to user.", parameter = "database")
@Option(name = "mysqlPort", description = "The MySQL server port number.", parameter = "port")
public class DataSourceFactory {

	private String user = null;
	private String password = null;
	private String serverName = null;
	private String databaseName = null;
	private int port = 3306;

	public DataSourceFactory(final Options<String> options) {
		withOptions(options);
	}

	public DataSourceFactory withOptions(final Options<String> options) {
		options.get("mysqlUser").value().ifPresent(this::withUser);
		options.get("mysqlPassword").value().ifPresent(this::withPassword);
		options.get("mysqlHostname").value().ifPresent(this::withServerName);
		options.get("mysqlDatabase").value().ifPresent(this::withDatabaseName);
		options.get("mysqlPort").parse().asInteger().ifPresent(this::withPort);
		return this;
	}

	public DataSourceFactory withUser(final String user) {
		this.user = user;
		return this;
	}

	public DataSourceFactory withPassword(final String password) {
		this.password = password;
		return this;
	}

	public DataSourceFactory withServerName(final String serverName) {
		this.serverName = serverName;
		return this;
	}

	public DataSourceFactory withDatabaseName(final String databaseName) {
		this.databaseName = databaseName;
		return this;
	}

	public DataSourceFactory withPort(final int port) {
		this.port = port;
		return this;
	}

	public DataSource create() {
		final MysqlDataSource ds = new MysqlDataSource();

		ds.setUser(user);
		ds.setPassword(password);
		ds.setServerName(serverName);
		ds.setDatabaseName(databaseName);
		ds.setPort(port);

		return ds;
	}
}
