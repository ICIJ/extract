package org.icij.extract.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FilenameUtils;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Locale;

@Option(name = "mysqlUser", description = "The MySQL database user to authenticate as.", parameter = "username")
@Option(name = "mysqlPassword", description = "The MySQL user password.", parameter = "password")
@Option(name = "mysqlHostname", description = "The MySQL server hostname.", parameter = "hostname")
@Option(name = "mysqlDatabase", description = "The MySQL database to user.", parameter = "database")
@Option(name = "mysqlPort", description = "The MySQL server port number.", parameter = "port")
@Option(name = "mysqlSSL", description = "Explicitly disable or require SSL, which the client will try and use by " +
		"default but not require.")
@Option(name = "mysqlCACertificate", description = "A trust store containing a CA certificate, used to verify the " +
		"MySQL server's certificate.", parameter = "path")
@Option(name = "mysqlCACertificatePassword", description = "The password for the CA certificate trust store.",
		parameter = "password")
public class DataSourceFactory {

	private String user = null;
	private String password = null;
	private String serverName = null;
	private String databaseName = null;
	private int port = 0;
	private boolean useSSL = false;
	private boolean requireSSL = false;
	private Path caCertificate = null;
	private String caPassword = null;
	private int maximumPoolSize = 0;

	public DataSourceFactory(final Options<String> options) {
		withOptions(options);
	}

	public DataSourceFactory withOptions(final Options<String> options) {
		options.get("mysqlUser").value().ifPresent(this::withUser);
		options.get("mysqlPassword").value().ifPresent(this::withPassword);
		options.get("mysqlHostname").value().ifPresent(this::withServerName);
		options.get("mysqlDatabase").value().ifPresent(this::withDatabaseName);
		options.get("mysqlPort").parse().asInteger().ifPresent(this::withPort);
		options.get("mysqlSSL").parse().asBoolean().ifPresent(this::withSSL);
		options.get("mysqlCACertificate").parse().asPath().ifPresent(this::withCACertificate);
		options.get("mysqlCACertificatePassword").value().ifPresent(this::withCACertificatePassword);
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

	public DataSourceFactory withSSL(final boolean useSSL) {
		this.useSSL = useSSL;
		if (useSSL) {
			requireSSL = true;
		}

		return this;
	}

	public DataSourceFactory withCACertificate(final Path path) {
		this.caCertificate = path;
		return this;
	}

	public DataSourceFactory withCACertificatePassword(final String password) {
		this.caPassword = password;
		return this;
	}

	public DataSourceFactory withMaximumPoolSize(final int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
		return this;
	}

	public HikariDataSource create(final String poolName) {
		final HikariConfig config = new HikariConfig();

		config.setJdbcUrl("jdbc:mysql://" + (null == serverName ? "localhost" : serverName) + ":" +
				(port > 0 ? port : 3306) + "/" + (null	== databaseName ? "extract" : databaseName));
		config.setUsername(null == user ? "extract" : user);
		config.setPassword(password);
		config.setPoolName(poolName);

		if (maximumPoolSize > 0) {
			config.setMaximumPoolSize(maximumPoolSize);
		}

		config.addDataSourceProperty("cachePrepStmts", true);
		config.addDataSourceProperty("prepStmtCacheSize", 250);
		config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
		config.addDataSourceProperty("autoCommit", true);
		config.addDataSourceProperty("useSSL", useSSL);
		config.addDataSourceProperty("requireSSL", requireSSL);

		if (null != caCertificate) {
			String keyStoreType = FilenameUtils.getExtension(caCertificate.getFileName().toString()
					.toUpperCase(Locale.ROOT));

			if (keyStoreType.isEmpty()) {
				keyStoreType = KeyStore.getDefaultType();
			} else if (keyStoreType.equals("P12")) {
				keyStoreType = "PKCS12";
			}

			config.addDataSourceProperty("verifyServerCertificate", true);
			config.addDataSourceProperty("trustCertificateKeyStoreType", keyStoreType);
			config.addDataSourceProperty("trustCertificateKeyStoreUrl", "file://" + caCertificate.toString());

			if (null != caPassword) {
				config.addDataSourceProperty("trustCertificateKeyStorePassword", caPassword);
			} else if (keyStoreType.equals("JKS")) {
				config.addDataSourceProperty("trustCertificateKeyStorePassword", "changeit");
			}
		}

		return new HikariDataSource(config);
	}
}
