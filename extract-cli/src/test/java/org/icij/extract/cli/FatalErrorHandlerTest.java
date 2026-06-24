package org.icij.extract.cli;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.fest.assertions.Assertions.assertThat;

public class FatalErrorHandlerTest {

	@Before
	public void resetFatalFlag() {
		Main.FATAL_ERROR_FLAGGED.set(false);
	}

	@Test
	public void testFatalErrorTriggersExitAction() {
		final AtomicBoolean exited = new AtomicBoolean(false);
		final Thread.UncaughtExceptionHandler handler = Main.fatalErrorHandler(() -> exited.set(true));

		handler.uncaughtException(Thread.currentThread(), new OutOfMemoryError("synthetic"));

		assertThat(exited.get()).isTrue();
		assertThat(Main.FATAL_ERROR_FLAGGED.get()).isTrue();
	}

	@Test
	public void testRecoverableErrorDoesNotTriggerExitAction() {
		final AtomicBoolean exited = new AtomicBoolean(false);
		final Thread.UncaughtExceptionHandler handler = Main.fatalErrorHandler(() -> exited.set(true));

		handler.uncaughtException(Thread.currentThread(), new StackOverflowError());

		assertThat(exited.get()).isFalse();
		assertThat(Main.FATAL_ERROR_FLAGGED.get()).isFalse();
	}

	@Test
	public void testExitCodeIsSeventy() {
		assertThat(Main.EXIT_FATAL_ERROR).isEqualTo(70);
	}
}
