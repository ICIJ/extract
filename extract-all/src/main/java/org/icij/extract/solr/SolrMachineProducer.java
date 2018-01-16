package org.icij.extract.solr;

import java.util.Set;

import java.util.function.Supplier;

import java.util.concurrent.Callable;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.LinkedTransferQueue;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.SolrServerException;

import org.icij.kaxxa.events.Notifiable;
import org.icij.extract.spewer.FieldNames;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Refactor into a Queue.
public class SolrMachineProducer extends StreamingResponseCallback implements Callable<Long>,
	Supplier<SolrDocument> {

	private static final Logger logger = LoggerFactory.getLogger(SolrMachineProducer.class);

	protected final TransferQueue<SolrDocument> queue = new LinkedTransferQueue<>();
	protected final SolrClient client;
	private final Set<String> fields;
	private Notifiable notifiable = null;

	private final int rows;
	private final int parallelism;

	private String idField = FieldNames.DEFAULT_ID_FIELD;
	private String filter = "*:*";

	private volatile boolean stopped = false;
	private long start = 0;
	private long found = 0;
	private long fetched = 0;

	public SolrMachineProducer(final SolrClient client, final Set<String> fields, final int parallelism) {
		this.client = client;
		this.parallelism = parallelism;
		this.rows = parallelism * 10;
		this.fields = fields;
	}

	SolrMachineProducer(final SolrClient client, final Set<String> fields) {
		this(client, fields, Runtime.getRuntime().availableProcessors());
	}

	public void setIdField(final String idField) {
		this.idField = idField;
	}

	String getIdField() {
		return idField;
	}

	public void setFilter(final String filter) {
		this.filter = filter;
	}

	public String getFilter() {
		return filter;
	}

	public void setNotifiable(final Notifiable notifiable) {
		this.notifiable = notifiable;
	}

	public Notifiable getNotifiable() {
		return notifiable;
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

		// For convenience on the consumer end, the poison pill instance is converted to null.
		if (document instanceof PoisonDocument) {
			return null;
		}

		return document;
	}

	@Override
	public Long call() throws IOException, SolrServerException, InterruptedException {
		long total = 0;

		try {
			while (!stopped && !Thread.currentThread().isInterrupted()) {
				total += fetch();
			}

		// Always poison: whether the thread exits in error or not, the transformers
		// still need to stop.
		} finally {
			poison();
		}

		return total;
	}

	@Override
	public void streamDocListInfo(final long found, final long start,
		final Float maxScore) {
		this.start = rows + this.start;

		// Update the progress bar if the number of items increases.
		if (null != notifiable && found > this.found) {
			notifiable.hintRemaining((int) found);
		}

		this.found = found;
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

	private long fetch() throws IOException, SolrServerException {
		final SolrQuery query = new SolrQuery(filter);

		query.setRows(rows);
		query.setStart((int) start);

		// Only request the fields to be copied and the ID.
		query.setFields(idField);

		if (null != fields) {
			fields.forEach(query::addField);
		}

		logger.info(String.format("Fetching up to %d documents, skipping %d.", rows, start));
		client.queryAndStreamResponse(query, this);

		final long fetched = this.fetched;

		// Stop if there are no more results.
		// Instruct transformers to stop by sending a poison pill.
		if (fetched < rows) {
			stopped = true;
		}

		// Reset for the next run.
		this.fetched = 0;
		return fetched;
	}

	private static class PoisonDocument extends SolrDocument {

		private static final long serialVersionUID = -5298876028754839466L;
	}
}
