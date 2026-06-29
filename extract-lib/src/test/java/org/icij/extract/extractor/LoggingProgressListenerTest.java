package org.icij.extract.extractor;

import org.junit.Test;
import java.nio.file.Paths;
import static org.fest.assertions.Assertions.assertThat;

public class LoggingProgressListenerTest {
    @Test public void testFormatLine() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 0L);
        p.incrementEmbeds(); p.incrementEmbeds();
        p.incrementOcrSubmitted(); p.incrementOcrSubmitted(); p.incrementOcrSubmitted();
        p.incrementOcrCompleted();
        assertThat(LoggingProgressListener.formatLine(p, 3_000L))
            .isEqualTo("/x.ost: 3s, 2 embeds, 1/3 OCR done");
    }
}
