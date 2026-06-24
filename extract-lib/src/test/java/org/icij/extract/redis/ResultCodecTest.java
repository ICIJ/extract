package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.Report;
import org.junit.Test;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Round-trips reports through {@link ResultEncoder}/{@link ResultDecoder} directly (no Redis),
 * pinning that multi-digit status codes survive the codec.
 */
public class ResultCodecTest {

    private final ResultEncoder encoder = new ResultEncoder();
    private final ResultDecoder decoder = new ResultDecoder();

    private Report roundTrip(final Report report) throws IOException {
        final ByteBuf encoded = encoder.encode(report);
        try {
            return (Report) decoder.decode(encoded, null);
        } finally {
            encoded.release();
        }
    }

    @Test
    public void testSingleDigitStatusRoundTrips() throws IOException {
        assertThat(roundTrip(new Report(ExtractionStatus.SUCCESS)).getStatus())
                .isEqualTo(ExtractionStatus.SUCCESS);
    }

    @Test
    public void testTwoDigitFailureTimeoutRoundTrips() throws IOException {
        assertThat(roundTrip(new Report(ExtractionStatus.FAILURE_TIMEOUT)).getStatus())
                .isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
    }

    @Test
    public void testTwoDigitFailureFatalRoundTrips() throws IOException {
        assertThat(roundTrip(new Report(ExtractionStatus.FAILURE_FATAL)).getStatus())
                .isEqualTo(ExtractionStatus.FAILURE_FATAL);
    }

    @Test
    public void testTwoDigitStatusWithExceptionRoundTrips() throws IOException {
        final Report decoded = roundTrip(new Report(ExtractionStatus.FAILURE_TIMEOUT, new RuntimeException("boom")));

        assertThat(decoded.getStatus()).isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
        assertThat(decoded.getException().isPresent()).isTrue();
        assertThat(decoded.getException().get().getMessage()).isEqualTo("boom");
    }
}
