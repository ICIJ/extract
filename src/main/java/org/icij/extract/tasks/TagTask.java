package org.icij.extract.tasks;

import org.icij.extract.solr.*;
import org.icij.net.http.PinnedHttpClientBuilder;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.util.Optional;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import org.icij.task.MonitorableTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tag the intersect or complement of two Solr cores, or a single core.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Tag the intersect or complement of two Solr cores, or a single core.\n\n" +
		"An intersect subset is calculated by iterating over documents in the core specified by the " +
		"\033[1m-a\033[0m option and checking whether they exist in the core specified by the " +
		"\033[1m-b\033[0m option.\n\n" +
		"A complement subset consists of those documents in the core specified by the \033[1m-a\033[0m option " +
		"which are not present in the core specified by the \033[1m-b\033[0m option.\n\n" +
		"Use literals to tag the documents in the core specified by the \033[1m-s\033[0m option, for example, " +
		"\"batch:1\".")
@Option(name = "index-type", description = "Specify the index type. For now, the only valid value is " +
		"\"solr\" (the default).", parameter = "type")
@Option(name = "address", description = "Index core API endpoint address.", code = "s", parameter = "url")
@Option(name = "server-certificate", description = "The index server's public certificate, used for " +
		"certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "verify-host", description = "Verify the index server's public certificate against the " +
		"specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
@Option(name = "filter", description = "Filter for documents to tag.", parameter = "query", code = "f")
@Option(name = "id-field", description = "Index field for an automatically generated identifier. The ID " +
		"for the same file is guaranteed not to change if the path doesn't change. Defaults to \"id\".", code = "i",
		parameter = "name")
@Option(name = "jobs", description = "The number of documents to process at a time. Defaults to the number" +
		" of available processors.", parameter = "number")
@Option(name = "commit", description = "Perform a commit when done.", code = "c")
@Option(name = "soft-commit", description = "Performs a soft commit. Makes index changes visible while " +
		"neither fsync-ing index files nor writing a new index descriptor. This could lead to data loss if Solr is " +
		"terminated unexpectedly.")
@Option(name = "subset-mode", description = "The mode to use if calculating a subset of two cores. When " +
		"specified, either \"intersection\" or \"complement\".", parameter = "mode")
@Option(name = "a-address", description = "Address of the first Solr core in the set. This should be the " +
		"smaller of the two cores, as iteration will occur over this set.", code = "a", parameter = "address")
@Option(name = "b-address", description = "Address of the second Solr core in the set. This should be the " +
		"larger of the two cores.", parameter = "address", code = "b")
public class TagTask extends MonitorableTask<Long> {

	private static final Logger logger = LoggerFactory.getLogger(TagTask.class);

	@Override
	public Long run(final String[] literals) throws Exception {
		final Map<String, String> pairs = new HashMap<>();

		if (null == literals || 0 == literals.length) {
			throw new IllegalArgumentException("You must pass literals as an argument.");
		}

		for (String literal : literals) {
			String[] pair = literal.split(":", 2);

			if (2 == pair.length) {
				pairs.put(pair[0], pair[1]);
			} else {
				throw new IllegalArgumentException(String.format("Invalid literal pair: \"%s\".", literal));
			}
		}

		return tag(pairs);
	}

	@Override
	public Long run() throws Exception {
		return run(null);
	}

	private Long tag(final Map<String, String> pairs) throws Exception {
		final int parallelism = options.get("jobs").parse().asInteger()
				.orElse(Runtime.getRuntime().availableProcessors());
		final String subsetMode = options.get("subset-mode").value().orElse(null);

		if (null != subsetMode && !(subsetMode.equals("intersection") || subsetMode.equals("complement"))) {
			throw new IllegalArgumentException(String.format("Invalid mode: \"%s\".", subsetMode));
		}

		final String addressA = options.get("a-address").value().orElse(null);
		final String addressB = options.get("b-address").value().orElse(null);

		if (null != subsetMode && (null == addressA || null == addressB)) {
			throw new IllegalArgumentException("Both cores of the set must be specified if operating " +
				"on an intersection or complement.");
		}

		final Optional<String> idField = options.get("id-field").value();
		final Optional<String> filter = options.get("filter").value();

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
			final SolrMachineConsumer consumer;
			final SolrMachineProducer producer;
			final long processed;

			if (null != subsetMode) {
				try (
					final SolrClient clientA = new HttpSolrClient.Builder(addressA).withHttpClient(httpClient).build();
					final SolrClient clientB = new HttpSolrClient.Builder(addressB).withHttpClient(httpClient).build()
				) {
					producer = new SolrMachineProducer(clientA, null, parallelism);

					if (subsetMode.equals("intersection")) {
						consumer = new SolrIntersectionConsumer(clientB, client, pairs);
					} else {
						consumer = new SolrComplementConsumer(clientB, client, pairs);
					}

					if (idField.isPresent()) {
						consumer.setIdField(idField.get());
						producer.setIdField(idField.get());
					}

					filter.ifPresent(producer::setFilter);

					final SolrMachine machine = new SolrMachine(consumer, producer, parallelism);

					consumer.setNotifiable(monitor);
					producer.setNotifiable(monitor);

					processed = machine.call();
					machine.terminate();
				}
			} else {
				consumer = new SolrTaggingConsumer(client, pairs);
				producer = new SolrMachineProducer(client, null, parallelism);

				if (idField.isPresent()) {
					consumer.setIdField(idField.get());
					producer.setIdField(idField.get());
				}

				filter.ifPresent(producer::setFilter);

				final SolrMachine machine = new SolrMachine(consumer, producer, parallelism);

				consumer.setNotifiable(monitor);
				producer.setNotifiable(monitor);

				processed = machine.call();
				machine.terminate();
			}

			logger.info(String.format("Processed a total of %d documents.", processed));
			logger.info(String.format("Tagged %d documents.", consumer.getConsumeCount()));

			if (options.get("soft-commit").parse().isOn()) {
				client.commit(true, true, true);
			} else if (options.get("commit").parse().isOn()) {
				client.commit(true, true, false);
			}

			return processed;
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to tag.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to tag because of an error while communicating with Solr.", e);
		}
	}
}
