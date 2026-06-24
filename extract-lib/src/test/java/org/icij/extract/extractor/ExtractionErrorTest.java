package org.icij.extract.extractor;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractionErrorTest {

	@Test
	public void testPreservesCause() {
		final OutOfMemoryError oom = new OutOfMemoryError("heap");
		final ExtractionError error = new ExtractionError(oom);
		assertThat(error.getCause()).isSameAs(oom);
	}

	@Test
	public void testIsAnException() {
		assertThat((Object) new ExtractionError(new StackOverflowError())).isInstanceOf(Exception.class);
	}

	@Test
	public void testMessageDescribesCause() {
		final ExtractionError error = new ExtractionError(new StackOverflowError());
		assertThat(error.getMessage()).contains("StackOverflowError");
	}
}
