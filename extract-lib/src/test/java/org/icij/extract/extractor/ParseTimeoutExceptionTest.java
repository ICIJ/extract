package org.icij.extract.extractor;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.fest.assertions.Assertions.assertThat;

public class ParseTimeoutExceptionTest {

    @Test
    public void testIsAnIOException() {
        assertThat(new ParseTimeoutException(Paths.get("x"), Duration.ofSeconds(5)))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testCarriesPathAndTimeoutAndMessage() {
        final Path path = Paths.get("doc.pdf");
        final ParseTimeoutException e = new ParseTimeoutException(path, Duration.ofMinutes(2));

        assertThat(e.getPath().toString()).isEqualTo(path.toString());
        assertThat(e.getTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(e.getMessage()).contains("doc.pdf");
        assertThat(e.getMessage()).contains("PT2M");
    }
}
