package org.icij.extract.extractor;

import org.junit.Test;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractionErrorsTest {

	@Test
	public void testOutOfMemoryIsFatal() {
		assertThat(ExtractionErrors.isFatal(new OutOfMemoryError())).isTrue();
	}

	@Test
	public void testInternalErrorIsFatal() {
		assertThat(ExtractionErrors.isFatal(new InternalError())).isTrue();
	}

	@Test
	public void testStackOverflowIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new StackOverflowError())).isFalse();
	}

	@Test
	public void testLinkageErrorIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new LinkageError())).isFalse();
	}

	@Test
	public void testRuntimeExceptionIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new RuntimeException())).isFalse();
	}

	@Test
	public void testCheckedExceptionIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new IOException())).isFalse();
	}

	@Test
	public void testAsExceptionPassesExceptionThrough() {
		final IOException io = new IOException("io");
		assertThat(ExtractionErrors.asException(io)).isSameAs(io);
	}

	@Test
	public void testAsExceptionWrapsError() {
		final StackOverflowError soe = new StackOverflowError();
		final Exception wrapped = ExtractionErrors.asException(soe);
		assertThat(wrapped).isInstanceOf(ExtractionError.class);
		assertThat(wrapped.getCause()).isSameAs(soe);
	}
}
