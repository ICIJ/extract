package org.icij.extract.extractor;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Thrown when a document's parse and output exceed the configured wall-clock timeout.
 * An {@link IOException} so it fits the {@code Extractor.extract(Path, Spewer)} signature
 * and is recorded as {@link ExtractionStatus#FAILURE_TIMEOUT} by the existing error path.
 */
public class ParseTimeoutException extends IOException {

    private final transient Path path;
    private final transient Duration timeout;

    public ParseTimeoutException(final Path path, final Duration timeout) {
        super(String.format("Parse exceeded %s timeout: \"%s\".", timeout, path));
        this.path = path;
        this.timeout = timeout;
    }

    public Path getPath() {
        return path;
    }

    public Duration getTimeout() {
        return timeout;
    }
}
