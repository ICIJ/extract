package org.icij.extract.core;

import java.util.Properties;

import java.io.File;

import org.apache.commons.io.FileUtils;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;

import org.junit.Test;
import org.junit.ClassRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.rules.TemporaryFolder;

public abstract class SolrJettyTestBase {

	public static final int DEFAULT_CONNECTION_TIMEOUT = 60000;

	public static JettySolrRunner jetty;
	public static int port;
	public static SolrClient client;

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
			.stopAtShutdown(true)
			.build();

		final Properties nodeProperties = new Properties();

		nodeProperties.setProperty("solr.data.dir", tempSolrData.getCanonicalPath());
		nodeProperties.setProperty("coreRootDirectory", tempSolrHome.toString());
		nodeProperties.setProperty("configSetBaseDir", tempSolrHome.toString());

		System.setProperty("jetty.testMode", "true");

		jetty = new JettySolrRunner(tempSolrHome.toString(), nodeProperties, jettyConfig);
		jetty.start();

		port = jetty.getLocalPort();
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

	public SolrClient getSolrClient() {
		if (null == client) {
			client = createNewSolrClient();
		}

		return client;
	}

	public SolrClient createNewSolrClient() {
		try {
			final String url = jetty.getBaseUrl().toString() + "/collection1";
			final HttpSolrClient client = new HttpSolrClient(url);

			client.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
			client.setDefaultMaxConnectionsPerHost(100);
			client.setMaxTotalConnections(100);

			return client;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
