package org.icij.extract.extractor;

import org.icij.concurrent.BlockingThreadPoolExecutor;
import org.icij.concurrent.ExecutorProxy;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.report.Reporter;
import org.icij.spewer.Spewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Base consumer for documents. Superclasses should call {@link #accept(TikaDocument)}. All tasks are sent to a
 * work-stealing thread pool.
 *
 * The parallelism of the thread pool is defined in the call to the constructor.
 *
 * A task is defined as both the extraction from a file and the output of extracted data.
 * Completion is only considered successful if both parts of the task complete with no exceptions.
 *
 * The final status of each task is saved to the reporter, if any is set.
 *
 * @since 1.0.0-beta
 */
public class DocumentConsumer extends ExecutorProxy implements Consumer<Path> {

	private static final Logger logger = LoggerFactory.getLogger(DocumentConsumer.class);

	protected final Spewer spewer;
	protected final Extractor extractor;

	/**
	 * The {@code Reporter} that will receive extraction results.
	 */
	private Reporter reporter = null;

	/**
	 * Returns the default thread pool size, which is equivalent to the number of available processors minus 1, or 1
	 * - whichever is greater.
	 *
	 * @return the default pool size
	 */
	public static int defaultPoolSize() {
		return Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
	}

	/**
	 * Create a new consumer that submits tasks to the given {@code Executor}.
	 *
	 * @param spewer the {@code Spewer} used to write extracted text and metadata
	 * @param extractor the {@code Extractor} used to extract from files
	 * @param executor the executor used to run consuming tasks
	 */
	public DocumentConsumer(final Spewer spewer, final Extractor extractor, final ExecutorService executor) {
		super(executor);
		this.spewer = spewer;
		this.extractor = extractor;
	}

	/**
	 * Create a new consumer with the given pool size. Uses a {@link BlockingThreadPoolExecutor}, which means that calls
	 * to {@link #accept} will block when the thread pool is full of running tasks.
	 *
	 * @param spewer the {@code Spewer} used to write extracted text and metadata
	 * @param extractor the {@code Extractor} used to extract from files
	 * @param poolSize the fixed size of the thread pool used to consume documents
	 */
	public DocumentConsumer(final Spewer spewer, final Extractor extractor, final int poolSize) {
		this(spewer, extractor, new BlockingThreadPoolExecutor(poolSize));
	}

	/**
	 * Create a new consumer with the default pool size, which is the number of available processors.
	 *
	 * @param spewer the {@code Spewer} used to write extracted text and metadata
	 * @param extractor the {@code Extractor} used to extract from files
	 */
	public DocumentConsumer(final Spewer spewer, final Extractor extractor) {
		this(spewer, extractor, defaultPoolSize());
	}

	/**
	 * Set the reporter.
	 *
	 * @param reporter reporter
	 */
	public void setReporter(final Reporter reporter) {
		this.reporter = reporter;
	}

	/**
	 * Get the reporter.
	 *
	 * @return The reporter.
	 */
	public Reporter getReporter() {
		return reporter;
	}

	/**
	 * Consume a file.
	 *
	 * If a blocking executor such as {@link BlockingThreadPoolExecutor} is being used (the default when no
	 * {@link ExecutorService} is passed to the constructor) then this method will block until a thread becomes
	 * available. Otherwise the behaviour is similar to {@link ExecutorService#execute(Runnable)}, causing the task
	 * to be put in a queue.
	 *
	 * @param path the tikaDocument to consume
	 * @throws RejectedExecutionException if unable to queue the consumer task for execution, including when the
	 * current thread is interrupted.
	 */
	@Override
	public void accept(final Path path) {
		logger.info(String.format("Sending to thread pool; will queue if full: \"%s\".", path));
		executor.execute(()-> {
			logger.info(String.format("Beginning extraction: \"%s\".", path));

			try {
				if (null != reporter) {
					extractor.extract(path, spewer, reporter);
				} else {
					extractor.extract(path, spewer);
				}
			} catch (Exception e) {
				logger.error(String.format("Exception while consuming file: \"%s\".", path), e);
			}
		});
	}

	@Override
	public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
		boolean result = super.awaitTermination(timeout, unit);
		try {
            spewer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
		return result;
    }
}
