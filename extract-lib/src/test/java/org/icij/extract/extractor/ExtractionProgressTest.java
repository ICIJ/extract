package org.icij.extract.extractor;

import org.junit.Test;
import java.nio.file.Paths;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class ExtractionProgressTest {
    @Test public void testCountersStartAtZeroAndIncrement() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        assertEquals(p.path(), Paths.get("/x.ost"));
        assertThat(p.embedsParsed()).isEqualTo(0L);
        p.incrementEmbeds();
        p.incrementOcrSubmitted();
        p.incrementOcrSubmitted();
        p.incrementOcrCompleted();
        assertThat(p.embedsParsed()).isEqualTo(1L);
        assertThat(p.ocrSubmitted()).isEqualTo(2L);
        assertThat(p.ocrCompleted()).isEqualTo(1L);
    }

    @Test public void testElapsedIsNowMinusStart() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        assertThat(p.elapsedMillis(4_000L)).isEqualTo(3_000L);
    }
}
