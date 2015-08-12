package org.icij.extract.solr;

import java.util.Set;

import java.util.function.Supplier;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.LinkedTransferQueue;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.SolrServerException;

public class SolrMachineProducer extends StreamingResponseCallback implements Callable<Integer>,
	Supplier<SolrDocument> {

	protected final TransferQueue<SolrDocument> queue = new LinkedTransferQueue<SolrDocument>();
	protected final Logger logger;
	protected final SolrClient client;
	protected Set<String> fields;

	private final int rows;
	private final int parallelism;

	private String idField = SolrDefaults.DEFAULT_ID_FIELD;

	private volatile boolean stopped = false;
	private int start = 0;
	private volatile int fetched = 0;

	public SolrMachineProducer(final Logger logger, final SolrClient client,
		final Set<String> fields, final int parallelism) {
		this.logger = logger;
		this.client = client;
		this.parallelism = parallelism;
		this.rows = parallelism;
		this.fields = fields;
	}

	public SolrMachineProducer(final Logger logger, final SolrClient client, final Set<String> fields) {
		this(logger, client, fields, Runtime.getRuntime().availableProcessors());
	}

	public void setIdField(final String idField) {
		this.idField = idField;
	}

	public String getIdField() {
		return idField;
	}

	@Override
	public SolrDocument get() {
		final SolrDocument document;

		try {
			document = queue.take();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		}

		// For convenience on the consumer end, the poison pill instance
		// is converted to null.
		if (document instanceof PoisonDocument) {
			return null;
		}

		return document;
	}

	@Override
	public Integer call() throws IOException, SolrServerException, InterruptedException {
		int total = 0;

		try {
			while (!stopped && !Thread.currentThread().isInterrupted()) {
				total += fetch();
			}

		// Always poison: whether the thread exits in error or not, the consumers
		// still need to stop.
		} finally {
			poison();
		}

		return new Integer(total);
	}

	@Override
	public void streamDocListInfo(final long found, final long start,
		final Float maxScore) {
		this.start = rows + this.start;
	}

	@Override
	public void streamSolrDocument(final SolrDocument document) {
		if (stopped) {
			return;
		}

		fetched++;
		try {

			// Throttle streaming by waiting for a slot to become free.
			queue.transfer(document);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			stopped = true;
		}
	}

	private void poison() throws InterruptedException {
		for (int i = 0; i < parallelism; i++) {
			queue.transfer(new PoisonDocument());
		}
	}

	private int fetch() throws IOException, SolrServerException, InterruptedException {
		final SolrQuery query = new SolrQuery("*:*");

		query.setRows(rows);
		query.setStart(start);

		// Only request the fields to be copied and the ID.
		query.setFields(idField);

		if (null != fields) {
			for (String field : fields) {
				query.addField(field);
			}
		}

		logger.info(String.format("Fetching up to %d documents, skipping %d.",
			rows, start));
		client.queryAndStreamResponse(query, this);

		final int fetched = this.fetched;

		// Stop if there are no more results.
		// Intruct consumers to stop by sending a poison pill.
		if (fetched < rows) {
			stopped = true;
		}

		// Reset for the next run.
		this.fetched = 0;
		return fetched;
	}

	private class PoisonDocument extends SolrDocument {

	}
}
