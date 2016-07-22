package org.icij.extract.test;

import java.util.Properties;

import java.io.File;

import java.net.URL;

import org.apache.commons.io.FileUtils;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;

import org.junit.ClassRule;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.rules.TemporaryFolder;

public abstract class SolrJettyTestBase extends TestBase {

	public static final int DEFAULT_CONNECTION_TIMEOUT = 60000;

	public static JettySolrRunner jetty;
	public static HttpSolrClient client;

	@ClassRule
	public static final TemporaryFolder tempSolrFolder = new TemporaryFolder();

	@BeforeClass
	public static void beforeSolrJettyTestBase() throws Exception {
		final File origSolrHome = new File(SolrJettyTestBase.class.getResource("/solr").toURI());
		final File tempSolrHome = tempSolrFolder.getRoot();
		final File tempSolrData = tempSolrFolder.newFolder("data");

		FileUtils.copyDirectory(origSolrHome, tempSolrHome);

		final JettyConfig jettyConfig = JettyConfig.builder()
			.setContext("/solr")
			.setPort(8888)
			.stopAtShutdown(true)
			.build();

		final Properties nodeProperties = new Properties();

		nodeProperties.setProperty("solr.data.dir", tempSolrData.getCanonicalPath());
		nodeProperties.setProperty("coreRootDirectory", tempSolrHome.toString());
		nodeProperties.setProperty("configSetBaseDir", tempSolrHome.toString());

		System.setProperty("jetty.testMode", "true");

		jetty = new JettySolrRunner(tempSolrHome.toString(), nodeProperties, jettyConfig);
		jetty.start();

		client = createNewSolrClient();
	}

	@AfterClass
	public static void afterSolrJettyTestBase() throws Exception {
		if (null != jetty) {
			jetty.stop();
			jetty = null;
		}

		if (null != client) {
			client.close();
			client = null;
		}
	}

	protected static HttpSolrClient createNewSolrClient() {
		return createNewSolrClient("collection1");
	}

	protected static HttpSolrClient createNewSolrClient(final String core) {
		try {
			URL url = jetty.getBaseUrl();

			// For debugging in Charles:
			// - set the proxy port to 8888
			// - enable transparent proxying
			// - enable Map Remote and map 127.0.0.1:8888 to 127.0.0.1:8080
			url = new URL(url.getProtocol(), url.getHost(), url.getPort(), url.getFile() + "/" + core);

			final HttpSolrClient client = new HttpSolrClient.Builder(url.toString()).build();

			client.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
			client.setDefaultMaxConnectionsPerHost(100);
			client.setMaxTotalConnections(100);

			return client;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
