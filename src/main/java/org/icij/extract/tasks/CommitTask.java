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
 * A task that sends a commit message to an indexer API endpoint.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Send a hard or soft commit message to the index.")
@Option(name = "index-type", description = "Specify the index type. For now, the only valid value is " +
		"\"solr\" (the default).", parameter = "type")
@Option(name = "soft-commit", description = "Performs a soft commit. Makes index changes visible while " +
		"neither fsync-ing index files nor writing a new index descriptor. This could lead to data loss if Solr is " +
		"terminated unexpectedly.")
@Option(name = "address", description = "Index core API endpoint address.", code = "s", parameter = "url")
@Option(name = "server-certificate", description = "The index server's public certificate, used for " +
		"certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "verify-host", description = "Verify the index server's public certificate against the " +
		"specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
public class CommitTask extends DefaultTask<Integer> {

	@Override
	public Integer run() throws Exception {
		final IndexType indexType = options.get("index-type").value(IndexType::parse).orElse(IndexType.SOLR);
		final boolean softCommit = options.get("soft-commit").parse().asBoolean().orElse(false);

		if (IndexType.SOLR == indexType) {
			return commitSolr(softCommit).getQTime();
		} else {
			throw new IllegalStateException("Not implemented.");
		}
	}

	/**
	 * Send a commit message to a Solr API endpoint.
	 *
	 * @param softCommit {@literal true} to commit without flushing changes to disk
	 * @return The response details from Solr.
	 */
	private UpdateResponse commitSolr(final boolean softCommit) {
		try (final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(options.get("verify-host").value().orElse(null))
				.pinCertificate(options.get("server-certificate").value().orElse(null))
				.build();
		     final SolrClient client = new HttpSolrClient.Builder(options.get("address").value().orElse
				     ("http://127.0.0.1:8983/solr/"))
				     .withHttpClient(httpClient)
				     .build()) {
			return client.commit(true, true, softCommit);
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to commit to Solr.", e);
		} catch (IOException e) {
			throw new RuntimeException("There was an error while communicating with Solr.", e);
		}
	}
}
