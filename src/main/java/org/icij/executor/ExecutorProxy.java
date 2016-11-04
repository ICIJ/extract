package org.icij.executor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class of traits used by implementing classes that proxy an executor.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0
 */
public abstract class ExecutorProxy {

	private static final Logger logger = LoggerFactory.getLogger(ExecutorProxy.class);

	/**
	 * The executor proxied by the implementing class.
	 */
	protected final ExecutorService executor;

	/**
	 * Instantiate a proxy for the given executor.
	 *
	 * @param executor the executor to proxy
	 */
	public ExecutorProxy(final ExecutorService executor) {
		this.executor = executor;
	}

	/**
	 * Shuts down the executor.
	 *
	 * This method should be called to free up resources when the executor is no longer needed. Running tasks will be
	 * allowed to complete but threads waiting on a blocking call to submit tasks to the executor will receive a
	 * {@link RejectedExecutionException} after this method is called.
	 */
	public void shutdown() {
		logger.info(String.format("Shutting down %s.", getClass().getName()));
		executor.shutdown();
	}

	/**
	 * Shut down the executor immediately, halting running tasks and discarding waiting tasks.
	 *
	 * @return list of tasks that never commenced execution if using an {@link ExecutorService} that allows tasks to
	 * be queued, otherwise an empty list if using a blocking executor.
	 */
	public List<Runnable> shutdownNow() {
		logger.info(String.format("Shutting down %s immediately.", getClass().getName()));
		return executor.shutdownNow();
	}

	/**
	 * Blocks until all the queued tasks have finished and the thread pool is empty, or the timeout is reached
	 * (whichever first).
	 *
	 * This method should be called to free up resources when the executor is no longer needed.
	 *
	 * @throws InterruptedException if interrupted while waiting
	 * @return {@code true} if the executor terminated before the given timeout, {@code false} otherwise.
	 */
	public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
		logger.info(String.format("Awaiting completion of %s up to %d %s.", getClass().getName(), timeout, unit));

		if (!executor.awaitTermination(timeout, unit)) {
			logger.warn(String.format("%s executor failed to terminate in %d %s.", getClass().getName(), timeout,
					unit));
			return false;
		}

		logger.info(String.format("%s terminated.", getClass().getName()));
		return true;
	}
}
