package org.icij.extract.mysql;

import org.apache.commons.io.FilenameUtils;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import javax.sql.DataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.usrz.libs.crypto.pem.PEMProvider;

import java.nio.file.Path;
import java.security.KeyStore;
import java.security.Security;
import java.util.Locale;
import java.util.Properties;

@Option(name = "mysqlUser", description = "The MySQL database user to authenticate as.", parameter = "username")
@Option(name = "mysqlPassword", description = "The MySQL user password.", parameter = "password")
@Option(name = "mysqlHostname", description = "The MySQL server hostname.", parameter = "hostname")
@Option(name = "mysqlDatabase", description = "The MySQL database to user.", parameter = "database")
@Option(name = "mysqlPort", description = "The MySQL server port number.", parameter = "port")
@Option(name = "mysqlCACertificate", description = "A CA certificate for the MySQL server.", parameter = "path")
public class DataSourceFactory {

	private String user = null;
	private String password = null;
	private String serverName = null;
	private String databaseName = null;
	private int port = 3306;
	private Path caCertificate = null;

	private boolean addedPEMProvider = false;

	public DataSourceFactory(final Options<String> options) {
		withOptions(options);
	}

	public DataSourceFactory withOptions(final Options<String> options) {
		options.get("mysqlUser").value().ifPresent(this::withUser);
		options.get("mysqlPassword").value().ifPresent(this::withPassword);
		options.get("mysqlHostname").value().ifPresent(this::withServerName);
		options.get("mysqlDatabase").value().ifPresent(this::withDatabaseName);
		options.get("mysqlPort").parse().asInteger().ifPresent(this::withPort);
		options.get("mysqlCACertificate").parse().asPath().ifPresent(this::withCACertificate);
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

	public DataSourceFactory withCACertificate(final Path path) {
		this.caCertificate = path;
		return this;
	}

	public DataSource create() {
		final MysqlDataSource ds = new MysqlDataSource();

		if (null != caCertificate) {
			final Properties properties = new Properties();
			String keyStoreType = FilenameUtils.getExtension(caCertificate.getFileName().toString()
					.toUpperCase(Locale.ROOT));

			if (keyStoreType.isEmpty()) {
				keyStoreType = KeyStore.getDefaultType();
			} else if (keyStoreType.equals("P12")) {
				keyStoreType = "PKCS12";
			} else if (keyStoreType.equals("PEM") && !addedPEMProvider) {
				addedPEMProvider = true;
				Security.addProvider(new PEMProvider());
			}

			properties.setProperty("verifyServerCertificate", "true");
			properties.setProperty("useSSL", "true");
			properties.setProperty("requireSSL", "true");
			properties.setProperty("trustCertificateKeyStoreType", keyStoreType);
			properties.setProperty("trustCertificateKeyStoreUrl", "file://" + caCertificate.toString());

			ds.initializeProperties(properties);
		}

		ds.setUser(null == user ? "extract" : user);
		ds.setPassword(password);
		ds.setServerName(serverName);
		ds.setDatabaseName(null == databaseName ? "extract" : databaseName);
		ds.setPort(port);

		return ds;
	}
}
