package org.icij.extract.parser;

import java.io.Serializable;
import java.time.Duration;
import org.apache.tika.parser.Parser;

public interface OCRConfigAdapter<C extends Serializable, P extends Parser> {
    void setLanguages(final String... languages);

    void setParsingTimeoutS(final int timeoutS);

    default void setOcrTimeout(final Duration duration) {
        setParsingTimeoutS((int) duration.toSeconds());
    }

    C getConfig();
    Class<P> getParserClass();
}
