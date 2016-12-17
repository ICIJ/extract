package org.icij.extract.solr;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

import java.util.function.Supplier;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrServerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A multi-threaded document-cycling robot for Solr.
 *
 * Multiple threads are used to consume from a streaming producer which runs from the current thread.
 *
 * Memory use is kept under control by throttling the streaming producer.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class SolrMachine implements Callable<Long> {

	private static final Logger logger = LoggerFactory.getLogger(SolrMachine.class);

	protected final SolrMachineConsumer consumer;
	protected final ExecutorService executor;

	private final SolrMachineProducer producer;
	private final int parallelism;

	public SolrMachine(final SolrMachineConsumer consumer,
		final SolrMachineProducer producer, final int parallelism) {
		this.consumer = consumer;
		this.producer = producer;
		this.parallelism = parallelism;
		this.executor = Executors.newFixedThreadPool(parallelism + 1);
	}

	public SolrMachine(final SolrMachineConsumer consumer, final SolrMachineProducer producer) {
		this(consumer, producer, Runtime.getRuntime().availableProcessors());
	}

	public void terminate() throws InterruptedException {
		logger.info("Shutting down Solr machine executor.");
		executor.shutdown();

		do {
			logger.info("Awaiting termination of Solr machine.");
		} while (!executor.awaitTermination(60, TimeUnit.SECONDS));
		logger.info("Solr machine terminated.");
	}

	@Override
	public Long call() throws IOException, SolrServerException, InterruptedException {
		final Collection<Callable<Long>> tasks = new ArrayList<>();

		// Add the producer to its own thread.
		tasks.add(producer);

		// Add the transformers - one per thread.
		for (int i = 0; i < parallelism; i++) {
			tasks.add(new Worker(producer));
		}

		final List<Future<Long>> futures = executor.invokeAll(tasks);
		long accepted = 0;

		try {
			futures.remove(0).get();

			for (Future<Long> task : futures) {
				accepted += task.get();
			}
		} catch (ExecutionException e) {
			final Throwable cause = e.getCause();

			if (cause instanceof SolrServerException) {
				throw (SolrServerException) cause;
			}

			if (cause instanceof IOException) {
				throw (IOException) cause;
			}

			throw new RuntimeException(cause);
		}

		return accepted;
	}

	private class Worker implements Callable<Long> {

		private final Supplier<SolrDocument> supplier;

		Worker(final Supplier<SolrDocument> supplier) {
			this.supplier = supplier;
		}

		@Override
		public Long call() throws Exception {
			long accepted = 0;

			while (!Thread.currentThread().isInterrupted()) {
				SolrDocument document = supplier.get();

				// Null value is used as a poison pull to parse workers to exit.
				if (null == document) {
					break;
				}

				try {
					consumer.accept(document);
					accepted++;

				// Log run-time exceptions and continue.
				} catch (RuntimeException e) {
					logger.error(String.format("Could not consume document: \"%s\".", document.getFieldValue(producer
							.getIdField())), e);
				}
			}

			return accepted;
		}
	}
}
