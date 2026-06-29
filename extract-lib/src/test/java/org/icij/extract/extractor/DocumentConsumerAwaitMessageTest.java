package org.icij.extract.extractor;

import org.junit.Test;
import java.nio.file.Paths;
import java.util.List;
import static org.fest.assertions.Assertions.assertThat;

public class DocumentConsumerAwaitMessageTest {
    @Test public void testSummaryNamesFilesAndElapsed() {
        ExtractionProgress a = new ExtractionProgress(Paths.get("/a.ost"), 0L);
        ExtractionProgress b = new ExtractionProgress(Paths.get("/b.ost"), 1_000L);
        String s = DocumentConsumer.inFlightSummary(List.of(a, b), 10_000L);
        assertThat(s).contains("/a.ost").contains("/b.ost").contains("10s").contains("9s");
    }

    @Test public void testEmptySummary() {
        assertThat(DocumentConsumer.inFlightSummary(List.of(), 0L)).isEqualTo("none");
    }
}
