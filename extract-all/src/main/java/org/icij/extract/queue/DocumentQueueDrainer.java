package org.icij.extract.queue;

import org.icij.kaxxa.concurrent.SealableLatch;
import org.icij.kaxxa.concurrent.ExecutorProxy;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Consumer;

import org.icij.extract.document.Document;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drains a queue by polling for documents to consume.
 *
 * @since 1.0.0
 */
@Option(name = "queuePoll", description = "Time to wait when polling the queue e.g. \"5s\" or \"1m\". " +
		"Defaults to 0.", parameter = "duration")
public class DocumentQueueDrainer extends ExecutorProxy {
	private static final Duration DEFAULT_TIMEOUT = Duration.ZERO;

	private final DocumentQueue queue;
	private final Consumer<Document> consumer;

	private SealableLatch latch = null;
	private Duration pollTimeout = DEFAULT_TIMEOUT;

	private static final Logger logger = LoggerFactory.getLogger(DocumentQueueDrainer.class);

	/**
	 * Create a new drainer that will drain documents from the given queue to the given consumer on a single thread.
	 *
	 * @param queue the queue to drain
	 * @param consumer must accept documents drained from the queue
	 */
	public DocumentQueueDrainer(final DocumentQueue queue, final Consumer<Document> consumer) {
		super(Executors.newSingleThreadExecutor());
		this.queue = queue;
		this.consumer = consumer;
	}

	public DocumentQueueDrainer configure(final Options<String> options) {
		options.get("queuePoll").parse().asDuration().ifPresent(this::setPollTimeout);
		return this;
	}

	/**
	 * Set the amount of time to wait until an item becomes available.
	 *
	 * @param pollTimeout the amount of time to wait
	 */
	public void setPollTimeout(final Duration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * Causes the consumer to wait until a new file is available, without any timeout.
	 *
	 * To not wait at all, call {@link #setPollTimeout(Duration pollTimeout)} with a value of {@code 0}.
	 */
	public void clearPollTimeout() {
		pollTimeout = null;
	}

	/**
	 * Get the poll timeout.
	 *
	 * @return The poll timeout.
	 */
	public Duration getPollTimeout() {
		return pollTimeout;
	}

	/**
	 * If given, the latch should be used to signal that the queue should be polled.
	 *
	 * {@linkplain SealableLatch#await()} will be called until polling returns a non-null value or the latch is sealed.
	 *
	 * @param latch the latch to await before polling
	 */
	public void setLatch(final SealableLatch latch) {
		this.latch = latch;
	}

	/**
	 * Get the poll latch.
	 *
	 * @return The poll latch.
	 */
	public SealableLatch getLatch() {
		return latch;
	}

	/**
	 * Clear the latch.
	 */
	public void clearLatch() {
		latch = null;
	}

	/**
	 * Drain the queue in a non-blocking way until the draining thread is interrupted, the task is cancelled or the
	 * given timeout is reached (if any is set).
	 *
	 * @return A {@link Future} representing the draining task that returns the number of documents consumed as a
	 * result.
	 */
	public Future<Long> drain() {
		return executor.submit(new DrainingTask());
	}

	/**
	 * Like {@link #drain()} except that draining will stop when the given poison pill is returned from the queue.
	 *
	 * @param poison the poison pill to test for when polling
	 * @see #drain()
	 */
	public Future<Long> drain(final Document poison) {
		return executor.submit(new DrainingTask(poison));
	}

	/**
	 * A {@link Callable} class that drains the queue until interrupted, a given poll timeout is reached or a task is
	 * rejected by the executor (indicating that it has been shutdown), or the given lock is released.
	 */
	private class DrainingTask implements Callable<Long> {

		/**
		 * The poison pill or null if constructed without one.
		 */
		private final Document poison;

		/**
		 * Instantiate a draining task that will drain the queue until {@link DocumentQueue#poll()} returns {@code null},
		 * or, if a timeout is specified, {@link DocumentQueue#poll(long, TimeUnit)} returns null after waiting.
		 *
		 * Note that if not timeout is specified, the draining thread will run until interrupted. If you want to
		 * signal it to stop, use {@link DrainingTask#DrainingTask(Document)} with a user-defined poison pill.
		 */
		DrainingTask() {
			this.poison = null;
		}

		/**
		 * Instantiate a draining task that will stop when {@link DocumentQueue#poll()} or
		 * {@link DocumentQueue#poll(long, TimeUnit)} returns {@code null} or the given poison pill. Poisoning a queue
		 * should be done with caution if the queue is shared by different processes.
		 *
		 * @param poison poison pill that will signal draining to stop
		 */
		DrainingTask(final Document poison) {
			this.poison = poison;
		}

		/**
		 * Poll the queue for a new file.
		 *
		 * Will wait for the duration set by {@link #setPollTimeout} or until a file becomes available if no timeout
		 * is set.
		 *
		 * If a {@link SealableLatch} is set, this method will await on that latch before polling and stop when the
		 * latch is sealed and signalled. Note that even if a signal is received, there is no guarantee that this
		 * method will not return {@code null} if a shared queue is being used.
		 *
		 * @throws InterruptedException if interrupted while polling
		 */
		private Document poll() throws InterruptedException {

			// Store the latch and timeout in local constants so that they be used in a thread-safe way.
			final Duration pollTimeout = getPollTimeout();
			final SealableLatch latch = getLatch();

			Document document;

			if (null != latch) {
				document = queue.poll();

				// Wait for a signal from the latch before polling again, but if a signal has been received and the
				// latch has been sealed in the meantime, break.
				while (null == document && !latch.isSealed()) {
					latch.await();
					document = queue.poll();
				}
			} else if (null == pollTimeout) {
				logger.info("Polling the queue, waiting indefinitely.");
				document = queue.take();
			} else if (pollTimeout.getSeconds() > 0) {
				logger.info(String.format("Polling the queue, waiting up to \"%s\".", pollTimeout));
				document = queue.poll(pollTimeout.getSeconds(), TimeUnit.SECONDS);
			} else {
				logger.info("Polling the queue without waiting.");
				document = queue.poll();
			}

			return document;
		}

		/**
		 * Drain the queue to the given consumer until polling returns {@code null} or a poison pill.
		 *
		 * @return The number of documents consumed.
		 * @throws InterruptedException if interrupted while polling
		 */
		private long drain() throws InterruptedException {
			long consumed = 0;

			Document document = poll();
			while (null != document && (null == poison || !document.equals(poison))) {
				consumer.accept(document);
				consumed++;
				document = poll();
			}

			return consumed;
		}

		@Override
		public Long call() throws Exception {
			logger.info("Draining to consumer until stopped or interrupted.");

			long consumed = drain();

			logger.info("Continuous draining stopped.");
			return consumed;
		}
	}
}
