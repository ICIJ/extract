package org.icij.extract.test;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.util.Properties;

public abstract class SolrJettyTestBase {

	private static final int DEFAULT_CONNECTION_TIMEOUT = 60000;

	private static JettySolrRunner jetty;

	protected static HttpSolrClient client;

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

	private static HttpSolrClient createNewSolrClient() {
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

			ModifiableSolrParams params = new ModifiableSolrParams();
			params.add(HttpClientUtil.PROP_CONNECTION_TIMEOUT, Integer.toString(DEFAULT_CONNECTION_TIMEOUT));

			return new HttpSolrClient.Builder(url.toString()).
					withHttpClient(HttpClientUtil.createClient(params)).build();
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
