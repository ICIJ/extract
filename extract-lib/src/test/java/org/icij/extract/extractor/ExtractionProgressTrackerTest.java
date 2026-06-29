package org.icij.extract.extractor;

import org.junit.Test;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import static org.fest.assertions.Assertions.assertThat;

public class ExtractionProgressTrackerTest {
    @Test public void testBeginEndMembership() {
        ExtractionProgressTracker t = new ExtractionProgressTracker(Duration.ofSeconds(60), () -> 0L);
        t.begin(Paths.get("/a.ost"));
        assertThat(t.inFlight()).hasSize(1);
        t.end(Paths.get("/a.ost"));
        assertThat(t.inFlight()).isEmpty();
    }

    @Test public void testTickNotifiesListenersWithInFlight() {
        ExtractionProgressTracker t = new ExtractionProgressTracker(Duration.ofSeconds(60), () -> 0L);
        List<Integer> seen = new ArrayList<>();
        t.addListener(inFlight -> seen.add(inFlight.size()));
        t.begin(Paths.get("/a.ost"));
        t.tick();
        assertThat(seen).containsExactly(1);
    }

    @Test public void testZeroIntervalStartDoesNotThrowAndCloses() {
        ExtractionProgressTracker t = new ExtractionProgressTracker(Duration.ZERO, () -> 0L);
        t.start();   // must be a no-op scheduler
        t.close();
    }
}
