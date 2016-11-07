package org.icij.extract.tasks;

import org.icij.extract.IndexType;
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

/**
 * Copy index fields.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Copy Solr fields from one field to another, or back to the same field to force reindexing.\n\n" +
		"Both literal mappings and wildcards are supported, for example \"field_a:field_b\" and \"field_*\".")
@Option(name = "index-type", description = "Specify the index type. For now, the only valid value is " +
		"\"solr\" (the default).", parameter = "type")
@Option(name = "address", description = "Index core API endpoint address.", code = "s", parameter = "url")
@Option(name = "server-certificate", description = "The index server's public certificate, used for " +
		"certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "verify-host", description = "Verify the index server's public certificate against the " +
		"specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
@Option(name = "commit", description = "Perform a commit when done.", code = "c")
@Option(name = "soft-commit", description = "Performs a soft commit. Makes index changes visible while " +
		"neither fsync-ing index files nor writing a new index descriptor. This could lead to data loss if Solr is " +
		"terminated unexpectedly.")
@Option(name = "id-field", description = "Index field for an automatically generated identifier. The ID " +
		"for the same file is guaranteed not to change if the path doesn't change. Defaults to \"id\".", code = "i",
		parameter = "name")
@Option(name = "filter", description = "Filter for documents to copy.", code = "f", parameter = "query")
@Option(name = "jobs", description = "The number of documents to process at a time. Defaults to the number" +
		" of available processors", parameter = "number")
public class CopyTask extends MonitorableTask<Long> {

	/**
	 * The default number of jobs to run.
	 */
	private static final int DEFAULT_JOBS = Runtime.getRuntime().availableProcessors();

	@Override
	public Long run(final String[] mappings) throws Exception {
		if (null == mappings || 0 == mappings.length) {
			throw new IllegalArgumentException("You must pass the field mappings on the command line.");
		}

		final Map<String, String> map = new HashMap<>();
		final int jobs = options.get("jobs").asInteger().orElse(DEFAULT_JOBS);
		final IndexType indexType = options.get("index-type").asEnum(IndexType::parse).orElse(IndexType.SOLR);

		for (String mapping : mappings) {
			String[] fields = mapping.split(":", 2);

			if (fields.length > 1) {
				map.put(fields[0], fields[1]);
			} else {
				map.put(fields[0], null);
			}
		}

		if (IndexType.SOLR == indexType) {
			return copySolr(map, jobs);
		} else {
			throw new IllegalStateException("Not implemented.");
		}
	}

	@Override
	public Long run() throws Exception {
		return run(null);
	}

	/**
	 * Copy the fields of a Solr index.
	 */
	private Long copySolr(final Map<String, String> map, final int jobs) throws Exception {
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
			final SolrMachineConsumer consumer = new SolrCopyConsumer(client, map);
			final SolrMachineProducer producer = new SolrMachineProducer(client, map.keySet(), jobs);
			final SolrMachine machine = new SolrMachine(consumer, producer, jobs);

			consumer.setNotifiable(monitor);
			producer.setNotifiable(monitor);

			final Optional<String> idField = options.get("id-field").value();

			if (idField.isPresent()) {
				consumer.setIdField(idField.get());
				producer.setIdField(idField.get());
			}

			final Optional<String> indexFilter = options.get("index-filter").value();

			if (indexFilter.isPresent()) {
				producer.setFilter(indexFilter.get());
			}

			final Long copied = machine.call();

			machine.terminate();

			if (options.get("soft-commit").on()) {
				client.commit(true, true, true);
			} else if (options.get("commit").on()) {
				client.commit(true, true, false);
			}

			return copied;
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to copy.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to copy because of an error while communicating with Solr.", e);
		}
	}
}
