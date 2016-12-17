package org.icij.extract.tasks;

import org.icij.extract.IndexType;
import org.icij.net.http.PinnedHttpClientBuilder;
import org.icij.task.MonitorableTask;

import java.io.IOException;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * Delete documents from the index.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Delete documents from Solr.\n\nSimple arguments are assumed to be IDs. Everything else is assumed to " +
		"be a query, for example \"path:data/*\" to delete all documents containing a \"path\" field value " +
		"starting with \"data/\".")
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
@Option(name = "commit", description = "Perform a commit when done.", code = "c")
@Option(name = "id-field", description = "Index field for an automatically generated identifier. The ID " +
		"for the same file is guaranteed not to change if the path doesn't change. Defaults to \"id\".", code = "i",
		parameter = "name")
public class DeleteTask extends MonitorableTask<Integer> {

	@Override
	public Integer run(final String[] queries) throws Exception {
		if (null == queries || 0 == queries.length) {
			throw new IllegalArgumentException("You must pass the queries or IDs to delete on the command line.");
		}

		final IndexType indexType = options.get("index-type").parse().asEnum(IndexType::parse).orElse(IndexType.SOLR);

		if (IndexType.SOLR != indexType) {
			throw new IllegalArgumentException("Not implemented.");
		}

		monitor.hintRemaining(queries.length);

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
					.setVerifyHostname(options.get("verify-host").value().orElse(null))
					.pinCertificate(options.get("server-certificate").value().orElse(null))
					.build();
			final SolrClient client = new HttpSolrClient.Builder(options.get("address").value().orElse
					("http://127.0.0.1:8983/solr/"))
					.withHttpClient(httpClient)
					.build()
		) {
			int queryTime = 0;

			for (String query : queries) {
				if (query.contains(":")) {
					queryTime += client.deleteByQuery(query).getQTime();
				} else {
					queryTime += client.deleteById(query).getQTime();
				}

				monitor.notifyListeners(query);
			}

			if (options.get("soft-commit").parse().isOn()) {
				client.commit(true, true, true);
			} else if (options.get("commit").parse().isOn()) {
				client.commit(true, true, false);
			}

			return queryTime;
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to delete.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to delete because of an error while communicating with Solr.", e);
		}
	}

	@Override
	public Integer run() throws Exception {
		return run(new String[]{"*:*"});
	}
}
