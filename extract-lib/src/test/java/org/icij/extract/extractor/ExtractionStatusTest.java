package org.icij.extract.extractor;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractionStatusTest {

	@Test
	public void testFailureFatalHasCodeTen() {
		assertThat(ExtractionStatus.FAILURE_FATAL.getCode()).isEqualTo(10);
	}

	@Test
	public void testParseFailureFatalByCode() {
		assertThat(ExtractionStatus.parse(10)).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}

	@Test
	public void testParseFailureFatalByName() {
		assertThat(ExtractionStatus.parse("FAILURE_FATAL")).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}

	@Test
	public void testFailureTimeoutCodeRoundTrips() {
		assertThat(ExtractionStatus.FAILURE_TIMEOUT.getCode()).isEqualTo(11);
		assertThat(ExtractionStatus.parse(11)).isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
		assertThat(ExtractionStatus.parse("FAILURE_TIMEOUT")).isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
	}
}
