package org.icij.extract.extractor;

import org.junit.Test;
import java.nio.file.Paths;
import static org.fest.assertions.Assertions.assertThat;

public class ExtractionProgressTest {
    @Test public void testCountersStartAtZeroAndIncrement() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        assertThat(p.path().toString()).isEqualTo(Paths.get("/x.ost").toString());
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

    @Test public void testEmbedsSkippedMaxDepthStartsAtZeroAndIncrements() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        assertThat(p.embedsSkippedMaxDepth()).isEqualTo(0L);
        p.incrementEmbedsSkippedMaxDepth();
        p.incrementEmbedsSkippedMaxDepth();
        assertThat(p.embedsSkippedMaxDepth()).isEqualTo(2L);
    }

    @Test public void testExpectedUnitsDefaultsToUnknown() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        assertThat(p.expectedUnits()).isEqualTo(-1L);
        assertThat(p.unitsParsed()).isEqualTo(0L);
        assertThat(p.parserTracksUnits()).isFalse();
    }

    @Test public void testSetExpectedUnitsIsSetOnce() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        assertThat(p.setExpectedUnits(250_000L)).isTrue();
        assertThat(p.expectedUnits()).isEqualTo(250_000L);
        // A second call (e.g. a nested container) must not clobber the root total.
        assertThat(p.setExpectedUnits(7L)).isFalse();
        assertThat(p.expectedUnits()).isEqualTo(250_000L);
    }

    @Test public void testIncrementUnitsAndFlag() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        p.incrementUnits(); p.incrementUnits();
        assertThat(p.unitsParsed()).isEqualTo(2L);
        p.markParserTracksUnits();
        assertThat(p.parserTracksUnits()).isTrue();
    }

    @Test public void testCompleteUnitsSnapsToTotal() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        p.setExpectedUnits(10L);
        p.incrementUnits(); p.incrementUnits(); p.incrementUnits();
        p.completeUnits();
        assertThat(p.unitsParsed()).isEqualTo(10L);
    }

    @Test public void testCompleteUnitsNoOpWhenUnknown() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        p.incrementUnits(); p.incrementUnits();
        p.completeUnits();
        assertThat(p.unitsParsed()).isEqualTo(2L);
        assertThat(p.expectedUnits()).isEqualTo(-1L);
    }
}
