package org.icij.extract.solr;

import java.util.Set;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

import java.util.function.Supplier;
import java.util.function.Consumer;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrServerException;

/**
 * A multithreaded document-cycling robot for Solr.
 *
 * Multiple threads are used to consume from a streaming producer which runs
 * from the current thread.
 *
 * Memory use is kept under control by throttling the streaming producer.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class SolrMachine implements Callable<Integer> {

	protected final Logger logger;
	protected final SolrMachineConsumer consumer;
	protected final SolrMachineProducer producer;
	protected final int parallelism;
	protected final ExecutorService executor;

	public SolrMachine(final Logger logger, final SolrMachineConsumer consumer,
		final SolrMachineProducer producer, final int parallelism) {
		this.logger = logger;
		this.consumer = consumer;
		this.producer = producer;
		this.parallelism = parallelism;
		this.executor = Executors.newFixedThreadPool(parallelism + 1);
	}

	public SolrMachine(final Logger logger, final SolrMachineConsumer consumer,
		final SolrMachineProducer producer) {
		this(logger, consumer, producer, Runtime.getRuntime().availableProcessors());
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
	public Integer call() throws IOException, SolrServerException, InterruptedException {
		final Collection<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();

		// Add the producer to its own thread.
		tasks.add(producer);

		// Add the consumers - one per thread.
		for (int i = 0; i < parallelism; i++) {
			tasks.add(new Worker(producer));
		}

		final List<Future<Integer>> futures = executor.invokeAll(tasks);
		int accepted = 0;

		try {
			futures.remove(0).get();

			for (Future<Integer> task : futures) {
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

		return new Integer(accepted);
	}

	protected class Worker implements Callable<Integer> {

		private final Supplier<SolrDocument> supplier;

		public Worker(final Supplier<SolrDocument> supplier) {
			this.supplier = supplier;
		}

		@Override
		public Integer call() throws Exception {
			int accepted = 0;

			while (!Thread.currentThread().isInterrupted()) {
				SolrDocument document = supplier.get();

				// Null value is used as a poison pull to get workers to exit.
				if (null == document) {
					break;
				}

				try {
					consumer.accept(document);
					accepted++;

				// Log run-time exceptions and continue.
				} catch (RuntimeException e) {
					logger.log(Level.SEVERE, String.format("Could not consume document: %s",
						document.getFieldValue(producer.getIdField())), e);
				}
			}

			return new Integer(accepted);
		}
	}
}
