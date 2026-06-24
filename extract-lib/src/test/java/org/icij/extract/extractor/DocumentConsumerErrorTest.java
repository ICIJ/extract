package org.icij.extract.extractor;

import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.fest.assertions.Assertions.assertThat;

public class DocumentConsumerErrorTest {

	/** An Extractor whose reporter-less extract throws a chosen throwable. */
	private static class ThrowingExtractor extends Extractor {
		private final Throwable toThrow;

		ThrowingExtractor(final Throwable toThrow) {
			this.toThrow = toThrow;
		}

		@Override
		public void extract(final Path path, final Spewer spewer) throws IOException {
			if (toThrow instanceof Error) {
				throw (Error) toThrow;
			}
			if (toThrow instanceof RuntimeException) {
				throw (RuntimeException) toThrow;
			}
			throw new IllegalStateException(toThrow);
		}
	}

	private Spewer nullSpewer() {
		return new PrintStreamSpewer(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
				new org.icij.spewer.FieldNames());
	}

	@Test
	public void testRecoverableErrorIsSwallowed() throws Exception {
		final AtomicReference<Throwable> uncaught = new AtomicReference<>();
		final java.util.concurrent.ExecutorService executor = newCapturingExecutor(uncaught);
		final DocumentConsumer consumer =
				new DocumentConsumer(nullSpewer(), new ThrowingExtractor(new StackOverflowError()), executor);

		consumer.accept(Paths.get("recoverable"));
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);

		assertThat(uncaught.get()).isNull();
	}

	@Test
	public void testFatalErrorPropagatesToUncaughtHandler() throws Exception {
		final AtomicReference<Throwable> uncaught = new AtomicReference<>();
		final java.util.concurrent.ExecutorService executor = newCapturingExecutor(uncaught);
		final OutOfMemoryError oom = new OutOfMemoryError("synthetic");
		final DocumentConsumer consumer =
				new DocumentConsumer(nullSpewer(), new ThrowingExtractor(oom), executor);

		consumer.accept(Paths.get("fatal"));
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);

		assertThat(uncaught.get()).isSameAs(oom);
	}

	private java.util.concurrent.ExecutorService newCapturingExecutor(final AtomicReference<Throwable> sink) {
		final ThreadFactory factory = runnable -> {
			final Thread thread = new Thread(runnable);
			thread.setUncaughtExceptionHandler((t, e) -> sink.set(e));
			return thread;
		};
		return java.util.concurrent.Executors.newSingleThreadExecutor(factory);
	}
}
