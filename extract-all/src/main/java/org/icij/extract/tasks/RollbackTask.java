package org.icij.extract.tasks;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.icij.extract.IndexType;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.icij.net.http.PinnedHttpClientBuilder;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * Task for sending a rollback message to the index.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Send a rollback message to the index.")
@Option(name = "indexType", description = "Specify the index type. For now, the only valid value is " +
		"\"solr\" (the default).", parameter = "type")
@Option(name = "address", description = "Index core API endpoint address.", code = "s", parameter = "url")
@Option(name = "serverCertificate", description = "The index server's public certificate, used for " +
		"certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "verifyHost", description = "Verify the index server's public certificate against the " +
		"specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
public class RollbackTask extends DefaultTask<Integer> {

	@Override
	public Integer run() throws Exception {
		final IndexType indexType = options.get("indexType").value(IndexType::parse).orElse(IndexType.SOLR);

		if (IndexType.SOLR == indexType) {
			return rollbackSolr().getQTime();
		} else {
			throw new IllegalStateException("Not implemented.");
		}
	}

	/**
	 * Send a rollback message to a Solr API endpoint.
	 *
	 * @return The response details from Solr.
	 */
	private UpdateResponse rollbackSolr() {
		try (final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(options.get("verifyHost").value().orElse(null))
				.pinCertificate(options.get("serverCertificate").value().orElse(null))
				.build();
		     final SolrClient client = new HttpSolrClient.Builder(options.get("address").value().orElse
				     ("http://127.0.0.1:8983/solr/"))
				     .withHttpClient(httpClient)
				     .build()) {
			return client.rollback();
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to roll back uncommitted documents.", e);
		} catch (IOException e) {
			throw new RuntimeException("There was an error while communicating with Solr.", e);
		}
	}
}
