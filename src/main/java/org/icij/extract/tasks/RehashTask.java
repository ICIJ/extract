package org.icij.extract.tasks;

import org.icij.extract.core.IndexDefaults;
import org.icij.extract.solr.*;
import org.icij.net.http.PinnedHttpClientBuilder;

import java.util.Collections;
import java.util.HashSet;

import java.io.IOException;
import java.util.Optional;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import org.icij.task.MonitorableTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * Task that allows the IDs of index documents to be rehashed, optionally replacing parts of the path at
 * the same time.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Recalculate IDs using a path replacement and/or new digest algorithm.")
@Option(name = "index-type", description = "Specify the index type. For now, the only valid value is " +
		"\"solr\" (the default).", parameter = "type")
@Option(name = "address", description = "Index core API endpoint address.", code = "s", parameter = "url")
@Option(name = "id-field", description = "Index field for an automatically generated identifier. The ID " +
		"for the same file is guaranteed not to change if the path doesn't change. Defaults to \"id\".", code = "i",
		parameter = "name")
@Option(name = "server-certificate", description = "The index server's public certificate, used for " +
		"certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "verify-host", description = "Verify the index server's public certificate against the " +
		"specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
@Option(name = "id-algorithm", description = "The hashing algorithm used for generating index document " +
		"identifiers from paths e.g. \"SHA-224\". Defaults to \"SHA-256\".", parameter = "algorithm")
@Option(name = "metadata-prefix", description = "Prefix for metadata fields added to the index. " +
		"Defaults to \"metadata_\".", parameter = "name")
@Option(name = "filter", description = "Filter for documents to rehash.", code = "f", parameter = "query")
@Option(name = "jobs", description = "The number of documents to process at a time. Defaults to the number" +
		" of available processors.", parameter = "number")
@Option(name = "commit", description = "Perform a commit when done.", code = "c")
@Option(name = "soft-commit", description = "Performs a soft commit. Makes index changes visible while " +
		"neither fsync-ing index files nor writing a new index descriptor. This could lead to data loss if Solr is " +
		"terminated unexpectedly.")
@Option(name = "pattern", description = "Replace part of the path using a regex pattern.", parameter =
		"pattern")
@Option(name = "replacement", description = "Replacement for the path regex. Defaults to an empty string" +
		".", parameter = "replacement")
public class RehashTask extends MonitorableTask<Long> {

	@Override
	public Long run(final String[] arguments) throws Exception {
		return run();
	}

	@Override
	public Long run() throws Exception {
		final int parallelism = options.get("jobs").integer().orElse(Runtime.getRuntime().availableProcessors());

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
			final SolrRehashConsumer consumer = new SolrRehashConsumer(client, options.get("id-algorithm").value()
					.orElse(IndexDefaults.DEFAULT_ID_ALGORITHM));
			final SolrMachineProducer producer = new SolrMachineProducer(client, new HashSet<>(Collections
					.singletonList("*")), parallelism);
			final SolrMachine machine = new SolrMachine(consumer, producer, parallelism);

			consumer.setNotifiable(monitor);
			producer.setNotifiable(monitor);

			final Optional<String> idField = options.get("id-field").value();
			final Optional<String> indexFilter = options.get("filter").value();
			final Optional<String> pattern = options.get("pattern").value();
			final Optional<String> replacement = options.get("replacement").value();

			if (idField.isPresent()) {
				consumer.setIdField(idField.get());
				producer.setIdField(idField.get());
			}

			if (indexFilter.isPresent()) {
				producer.setFilter(indexFilter.get());
			}

			if (pattern.isPresent()) {
				consumer.setPattern(pattern.get());
			}

			if (replacement.isPresent()) {
				consumer.setReplacement(replacement.get());
			}

			final long copied = machine.call();

			machine.terminate();

			if (options.get("soft-commit").on()) {
				client.commit(true, true, true);
			} else if (options.get("commit").on()) {
				client.commit(true, true, false);
			}

			return copied;
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to rehash.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to rehash because of an error while communicating with Solr.", e);
		}
	}
}
