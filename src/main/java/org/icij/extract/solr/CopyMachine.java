package org.icij.extract.solr;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.SolrServerException;

/**
 * A multithreaded field-copying robot for Solr.
 *
 * Multiple threads are used to consume from a streaming producer which runs
 * from the current thread.
 *
 * Memory use is kept under control by throttling the streaming producer.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class CopyMachine {
	public static final int PARALLELISM = Runtime.getRuntime().availableProcessors();
	public static final String DEFAULT_ID_FIELD = "id";
	public static final int DEFAULT_BATCH_SIZE = 100;

	private final Logger logger;
	private final SolrClient client;
	private final Map<String, String> map;

	private final Semaphore slots = new Semaphore(PARALLELISM);
	private final ExecutorService executor = Executors.newWorkStealingPool(PARALLELISM);

	private int rows = DEFAULT_BATCH_SIZE;
	private String idField = DEFAULT_ID_FIELD;
	private AtomicInteger copied = new AtomicInteger();
	private AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();

	public CopyMachine(final Logger logger, final SolrClient client, final Map<String, String> map) {
		this.logger = logger;
		this.client = client;
		this.map = map;
	}

	public void setIdField(final String idField) {
		this.idField = idField;
	}

	public String getIdField() {
		return idField;
	}

	public void setBatchSize(final int rows) {
		this.rows = rows;
	}

	public int getBatchSize() {
		return rows;
	}

	public void shutdown() {
		executor.shutdown();
	}

	public int copy() throws SolrServerException, IOException {
		final Producer producer = new Producer();

		producer.run();

		// Check whether the producer was interrupted.
		// If so, return early.
		if (Thread.currentThread().isInterrupted()) {
			return copied.get();
		}

		try {
			slots.acquire(PARALLELISM);

		// Return early if interrupted while waiting for consumers to finish.
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return copied.get();
		}

		final Throwable e = throwable.get();

		if (null == e) {
			logger.info("Copy completed successfully.");
			return copied.get();
		}

		if (e instanceof SolrServerException) {
			throw (SolrServerException) e;
		} else if (e instanceof IOException) {
			throw (IOException) e;
		} else {
			throw new RuntimeException(e);
		}
	}

	private class Producer extends StreamingResponseCallback implements Runnable {

		private boolean stopped = false;
		private int start = 0;
		private int position = 0;

		@Override
		public void run() {
			while (!stopped) {
				fetch();
			}
		}

		@Override
		public void streamDocListInfo(long found, long start, Float maxScore) {
			this.start = rows + this.start;

			// Stop execution as soon as the first exception is detected.
			if (null != throwable.get()) {
				stopped = true;
				return;
			}
		}

		@Override
		public void streamSolrDocument(final SolrDocument document) {
			if (stopped) {
				return;
			}

			position++;
			try {

				// Throttle streaming by waiting for a slot to become free.
				slots.acquire();
				executor.execute(new ConsumerTask(document));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				stopped = true;
			}
		}

		private void fetch() {
			final SolrQuery query = new SolrQuery("*:*");
	
			query.setRows(rows);
			query.setStart(start);

			// Only request the fields to be copied and the ID.
			query.setFields(idField);
			for (String from : map.keySet()) {
				query.addField(from);
			}

			logger.info(String.format("Fetching up to %d documents, skipping %d.",
				rows, start));

			try {
				client.queryAndStreamResponse(query, this);
			} catch (Throwable e) {
				if (!throwable.compareAndSet(null, e)) {
					throwable.get().addSuppressed(e);
				}
			}

			// Stop if there are no more results.
			if (position < rows) {
				logger.info("Stopping.");
				stopped = true;

			// Reset for the next run.
			} else {
				position = 0;
			}
		}
	}

	private class ConsumerTask implements Runnable {

		private final SolrDocument input;

		public ConsumerTask(final SolrDocument input) {
			this.input = input;
		}

		@Override
		public void run() {
			try {
				copy(input);
				copied.incrementAndGet();
			} catch (Throwable e) {
				if (!throwable.compareAndSet(null, e)) {
					throwable.get().addSuppressed(e);
				}
			} finally {
				slots.release();
			}
		}

		private void copyField(final String from, final SolrDocument input,
			final SolrInputDocument output) {
			final Map<String, Object> atomic = new HashMap<String, Object>();
			String to = map.get(from);

			// If there's no target field, copy the field onto itself.
			// This forces reindexing in Solr.
			if (null == to) {
				to = from;
			}

			// The ID field can't be set atomically.
			if (to.equals(idField)) {
				output.setField(to, input.getFieldValue(idField));
			} else {
				atomic.put("set", input.getFieldValue(from));
				output.setField(to, atomic);
			}
		}

		private void copy(final SolrDocument input) throws SolrServerException, IOException {
			final SolrInputDocument output = new SolrInputDocument(); 

			// Copy the source fields to the target fields.
			// Copy all the fields from the returned document. This ensures that
			// wildcard matches work.
			for (String field : input.keySet()) {
				copyField(field, input, output);
			}

			logger.info(String.format("Adding document with ID %s.",
				input.getFieldValue(idField)));
			client.add(output);
		}
	}
}
