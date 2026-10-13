# Parallel OCR and Progress Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make extract-lib parallelize image-attachment OCR across a bounded shared pool, emit a periodic extraction-progress heartbeat (plus a programmatic hook), and replace the misleading executor-await message with one that names in-flight files.

**Architecture:** A single `Extractor` instance (shared across all parse threads) owns one bounded OCR `ExecutorService` and one `ExtractionProgressTracker`. During the still-sequential container walk, `EmbedSpawner` digests image-leaf embeds synchronously (preserving IDs and artifact filenames) but defers their OCR text extraction to the shared pool, joining all futures before the spew. The tracker holds the in-flight map and feeds a daemon heartbeat that drives logging and any registered `ProgressListener`.

**Tech Stack:** Java 17+, Apache Tika, JUnit 4, fest-assert (`org.fest.assertions.Assertions.assertThat`), mockito-all, Maven. `org.icij.task.Options` for option parsing.

## Global Constraints

- All changes are in module `extract-lib`; no other module is modified by this plan.
- Embed IDs, `X-TIKA:digest:*` content hashes, and artifact filenames (`embed.getHash()`) MUST be byte-identical to today's serial behavior. This is the contract Datashare's on-demand `EmbeddedDocumentExtractor.extract` / `SourceExtractor` and the `artifactDir` cache depend on.
- `EmbeddedDocumentExtractor` (the on-demand single-embed walker) MUST NOT be modified.
- Default behavior change is allowed (`ocrParallelism` defaults to CPU count), but `ocrParallelism=1` OR `ocrFanout=false` MUST restore exact serial behavior.
- New options on `Extractor`, exact names and defaults: `ocrParallelism` (int, `Runtime.getRuntime().availableProcessors()`), `ocrFanout` (boolean, `true`), `ocrMinImageBytes` (long, `0`), `progressHeartbeatInterval` (duration, `60s`; `0` disables).
- OCR tasks are leaf image parses only; they never resubmit into the pool (no self-starvation).
- Run a single test class with: `mvn -pl extract-lib test -Dtest=ClassName`. Run the module suite with `mvn -pl extract-lib test`.
- Follow existing style: JUnit 4 (`@Test`, `@Before`, `@After`), fest-assert, package-private constructors in `org.icij.extract.extractor`.

---

### Task 1: `ExtractionProgress` value + counters

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgress.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTest.java`

**Interfaces:**
- Produces: `ExtractionProgress(Path path, long startMillis)`; methods `Path path()`, `long startMillis()`, `long embedsParsed()`, `long ocrSubmitted()`, `long ocrCompleted()`, `void incrementEmbeds()`, `void incrementOcrSubmitted()`, `void incrementOcrCompleted()`, `long elapsedMillis(long now)`.

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.extractor;

import org.junit.Test;
import java.nio.file.Paths;
import static org.fest.assertions.Assertions.assertThat;

public class ExtractionProgressTest {
    @Test public void testCountersStartAtZeroAndIncrement() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        assertThat(p.path()).isEqualTo(Paths.get("/x.ost"));
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ExtractionProgressTest`
Expected: FAIL (compilation error: `ExtractionProgress` does not exist).

- [ ] **Step 3: Write minimal implementation**

```java
package org.icij.extract.extractor;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Mutable, thread-safe per-file extraction progress snapshot. */
public class ExtractionProgress {
    private final Path path;
    private final long startMillis;
    private final AtomicLong embeds = new AtomicLong();
    private final AtomicLong ocrSubmitted = new AtomicLong();
    private final AtomicLong ocrCompleted = new AtomicLong();

    public ExtractionProgress(final Path path, final long startMillis) {
        this.path = path;
        this.startMillis = startMillis;
    }

    public Path path() { return path; }
    public long startMillis() { return startMillis; }
    public long embedsParsed() { return embeds.get(); }
    public long ocrSubmitted() { return ocrSubmitted.get(); }
    public long ocrCompleted() { return ocrCompleted.get(); }
    public void incrementEmbeds() { embeds.incrementAndGet(); }
    public void incrementOcrSubmitted() { ocrSubmitted.incrementAndGet(); }
    public void incrementOcrCompleted() { ocrCompleted.incrementAndGet(); }
    public long elapsedMillis(final long now) { return now - startMillis; }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=ExtractionProgressTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgress.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTest.java
git commit -m "feat(progress): add ExtractionProgress counter value object"
```

---

### Task 2: `ProgressListener` + `ExtractionProgressTracker`

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/ProgressListener.java`
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgressTracker.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTrackerTest.java`

**Interfaces:**
- Consumes: `ExtractionProgress` (Task 1).
- Produces:
  - `interface ProgressListener { void onHeartbeat(Collection<ExtractionProgress> inFlight); }`
  - `ExtractionProgressTracker(Duration heartbeatInterval, LongSupplier clock)` (test ctor) and `ExtractionProgressTracker(Duration heartbeatInterval)` (uses `System::currentTimeMillis`).
  - `ExtractionProgress begin(Path)`, `void end(Path)`, `Collection<ExtractionProgress> inFlight()`, `void addListener(ProgressListener)`, `void tick()` (package-visible, fires listeners once), `void start()` (schedules `tick()` every interval unless interval is zero), `void close()` (shuts the scheduler).

- [ ] **Step 1: Write the failing test**

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ExtractionProgressTrackerTest`
Expected: FAIL (compilation error: classes do not exist).

- [ ] **Step 3: Write minimal implementation**

`ProgressListener.java`:

```java
package org.icij.extract.extractor;

import java.util.Collection;

@FunctionalInterface
public interface ProgressListener {
    void onHeartbeat(Collection<ExtractionProgress> inFlight);
}
```

`ExtractionProgressTracker.java`:

```java
package org.icij.extract.extractor;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Tracks in-flight extractions and drives a periodic heartbeat to registered listeners. */
public class ExtractionProgressTracker implements AutoCloseable {
    private final ConcurrentHashMap<Path, ExtractionProgress> inFlight = new ConcurrentHashMap<>();
    private final List<ProgressListener> listeners = new CopyOnWriteArrayList<>();
    private final Duration heartbeatInterval;
    private final LongSupplier clock;
    private ScheduledExecutorService scheduler;

    public ExtractionProgressTracker(final Duration heartbeatInterval) {
        this(heartbeatInterval, System::currentTimeMillis);
    }

    public ExtractionProgressTracker(final Duration heartbeatInterval, final LongSupplier clock) {
        this.heartbeatInterval = heartbeatInterval;
        this.clock = clock;
    }

    public ExtractionProgress begin(final Path path) {
        final ExtractionProgress progress = new ExtractionProgress(path, clock.getAsLong());
        inFlight.put(path, progress);
        return progress;
    }

    public void end(final Path path) { inFlight.remove(path); }

    public Collection<ExtractionProgress> inFlight() { return inFlight.values(); }

    public void addListener(final ProgressListener listener) { listeners.add(listener); }

    void tick() {
        final Collection<ExtractionProgress> snapshot = inFlight();
        for (final ProgressListener listener : listeners) {
            listener.onHeartbeat(snapshot);
        }
    }

    public synchronized void start() {
        if (heartbeatInterval == null || heartbeatInterval.isZero() || heartbeatInterval.isNegative() || scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "extract-progress-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        final long millis = heartbeatInterval.toMillis();
        scheduler.scheduleAtFixedRate(this::tick, millis, millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=ExtractionProgressTrackerTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ProgressListener.java \
        extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgressTracker.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTrackerTest.java
git commit -m "feat(progress): add ExtractionProgressTracker with heartbeat scheduler"
```

---

### Task 3: `LoggingProgressListener`

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/LoggingProgressListener.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/LoggingProgressListenerTest.java`

**Interfaces:**
- Consumes: `ProgressListener`, `ExtractionProgress`.
- Produces: `LoggingProgressListener(LongSupplier clock)`; static helper `static String formatLine(ExtractionProgress p, long now)` returning e.g. `"/x.ost: 3s, 5 embeds, 2/3 OCR done"`.

- [ ] **Step 1: Write the failing test**

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=LoggingProgressListenerTest`
Expected: FAIL (class does not exist).

- [ ] **Step 3: Write minimal implementation**

```java
package org.icij.extract.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.LongSupplier;

/** Default progress listener: logs one INFO line per in-flight file each heartbeat. */
public class LoggingProgressListener implements ProgressListener {
    private static final Logger logger = LoggerFactory.getLogger(LoggingProgressListener.class);
    private final LongSupplier clock;

    public LoggingProgressListener() { this(System::currentTimeMillis); }
    public LoggingProgressListener(final LongSupplier clock) { this.clock = clock; }

    static String formatLine(final ExtractionProgress p, final long now) {
        final long seconds = p.elapsedMillis(now) / 1000L;
        return String.format("%s: %ds, %d embeds, %d/%d OCR done",
                p.path(), seconds, p.embedsParsed(), p.ocrCompleted(), p.ocrSubmitted());
    }

    @Override
    public void onHeartbeat(final Collection<ExtractionProgress> inFlight) {
        if (inFlight.isEmpty()) {
            return;
        }
        final long now = clock.getAsLong();
        for (final ExtractionProgress p : inFlight) {
            logger.info(formatLine(p, now));
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=LoggingProgressListenerTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/LoggingProgressListener.java \
        extract-lib/src/test/java/org/icij/extract/extractor/LoggingProgressListenerTest.java
git commit -m "feat(progress): add LoggingProgressListener default heartbeat output"
```

---

### Task 4: Add new options + shared OCR pool + tracker to `Extractor`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorOcrPoolConfigTest.java`

**Interfaces:**
- Consumes: `ExtractionProgressTracker`, `LoggingProgressListener` (Tasks 2-3).
- Produces (new on `Extractor`): `int getOcrParallelism()`, `boolean isOcrFanout()`, `long getOcrMinImageBytes()`, `ExecutorService getOcrExecutor()`, `ExtractionProgressTracker getProgressTracker()`, `void addProgressListener(ProgressListener)`, and `Extractor implements AutoCloseable` with `void close()`.

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.extractor;

import org.icij.task.Options;
import org.junit.Test;
import java.util.Map;
import static org.fest.assertions.Assertions.assertThat;

public class ExtractorOcrPoolConfigTest {
    @Test public void testDefaultsToCpuCountAndFanoutOn() {
        Extractor extractor = new Extractor();
        assertThat(extractor.getOcrParallelism()).isEqualTo(Runtime.getRuntime().availableProcessors());
        assertThat(extractor.isOcrFanout()).isTrue();
        assertThat(extractor.getOcrMinImageBytes()).isEqualTo(0L);
        extractor.close();
    }

    @Test public void testOptionsOverrideValues() {
        Extractor extractor = new Extractor(Options.from(Map.of(
            "ocrParallelism", "3", "ocrFanout", "false", "ocrMinImageBytes", "4096")));
        assertThat(extractor.getOcrParallelism()).isEqualTo(3);
        assertThat(extractor.isOcrFanout()).isFalse();
        assertThat(extractor.getOcrMinImageBytes()).isEqualTo(4096L);
        extractor.close();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ExtractorOcrPoolConfigTest`
Expected: FAIL (methods do not exist).

- [ ] **Step 3: Write minimal implementation**

Add `@Option` annotations near the other ones (after line 114, before `public class Extractor`):

```java
@Option(name = "ocrParallelism", description = "Number of OCR tasks run in parallel across all " +
        "extraction threads. Defaults to the number of available processors. Set to 1 for serial OCR.",
        parameter = "count")
@Option(name = "ocrFanout", description = "Enable parallel OCR fan-out for image attachments. " +
        "On by default. When off, images OCR inline on the parse thread.")
@Option(name = "ocrMinImageBytes", description = "Image embeds smaller than this many bytes OCR " +
        "inline instead of via the pool. Defaults to 0 (always fan out).", parameter = "bytes")
@Option(name = "progressHeartbeatInterval", description = "Interval between extraction progress " +
        "heartbeat log lines, e.g. \"60s\". Set to 0 to disable. Defaults to 60s.", parameter = "duration")
```

Add fields (next to the other private fields, after `parseExecutor`):

```java
    private int ocrParallelism = Runtime.getRuntime().availableProcessors();
    private boolean ocrFanout = true;
    private long ocrMinImageBytes = 0L;
    private Duration progressHeartbeatInterval = Duration.ofSeconds(60);
    private ExecutorService ocrExecutor;
    private ExtractionProgressTracker progressTracker;
```

In `configure(...)`, before the final `logger.info(...)` line, parse the options:

```java
        options.get("ocrParallelism", String.valueOf(Runtime.getRuntime().availableProcessors()))
                .parse().asInteger().ifPresent(n -> this.ocrParallelism = Math.max(1, n));
        options.get("ocrFanout", "true").parse().asBoolean().ifPresent(b -> this.ocrFanout = b);
        options.get("ocrMinImageBytes", "0").parse().asLong().ifPresent(b -> this.ocrMinImageBytes = b);
        options.get("progressHeartbeatInterval", "60s").parse().asDuration()
                .ifPresent(d -> this.progressHeartbeatInterval = d);
```

At the END of the `Extractor(DocumentFactory, Options)` constructor (after the `replaceParser(OutlookPSTParser.class, ...)` line), initialize the shared collaborators:

```java
        this.ocrExecutor = Executors.newFixedThreadPool(ocrParallelism, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            @Override public Thread newThread(final Runnable r) {
                final Thread thread = new Thread(r, "extract-ocr-" + counter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        });
        this.progressTracker = new ExtractionProgressTracker(progressHeartbeatInterval);
        this.progressTracker.addListener(new LoggingProgressListener());
        this.progressTracker.start();
```

Add accessors and `close()` (place near the other public methods):

```java
    public int getOcrParallelism() { return ocrParallelism; }
    public boolean isOcrFanout() { return ocrFanout; }
    public long getOcrMinImageBytes() { return ocrMinImageBytes; }
    public ExecutorService getOcrExecutor() { return ocrExecutor; }
    public ExtractionProgressTracker getProgressTracker() { return progressTracker; }
    public void addProgressListener(final ProgressListener listener) {
        progressTracker.addListener(listener);
    }
```

Change the class declaration `public class Extractor {` to `public class Extractor implements AutoCloseable {` and add:

```java
    @Override
    public void close() {
        if (ocrExecutor != null) { ocrExecutor.shutdownNow(); }
        if (progressTracker != null) { progressTracker.close(); }
        parseExecutor.shutdownNow();
    }
```

(If `org.icij.task.Options` has no `asLong()`, parse via `asInteger()` is unsafe for large byte counts; use `options.valueIfPresent("ocrMinImageBytes").map(Long::parseLong)` instead. Confirm by checking `org.icij.task.Options` before Step 4.)

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=ExtractorOcrPoolConfigTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorOcrPoolConfigTest.java
git commit -m "feat(ocr): add ocrParallelism/ocrFanout/ocrMinImageBytes options and shared OCR pool"
```

---

### Task 5: OCR-eligibility predicate in `EmbedSpawner`

This isolates the decision "should this embed be deferred to the OCR pool?" so it can be unit-tested before the harder deferral wiring.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerEligibilityTest.java`

**Interfaces:**
- Produces: `static boolean isOcrEligible(Metadata metadata, long minImageBytes)` on `EmbedSpawner` — true when `CONTENT_TYPE` starts with `image/`, the resource type is not `INLINE`, and (the embed size is unknown OR `>= minImageBytes`).

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.junit.Test;
import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerEligibilityTest {
    private Metadata image() {
        Metadata m = new Metadata();
        m.set(Metadata.CONTENT_TYPE, "image/png");
        return m;
    }

    @Test public void testImageLeafIsEligible() {
        assertThat(EmbedSpawner.isOcrEligible(image(), 0L)).isTrue();
    }

    @Test public void testNonImageIsNotEligible() {
        Metadata m = new Metadata();
        m.set(Metadata.CONTENT_TYPE, "application/pdf");
        assertThat(EmbedSpawner.isOcrEligible(m, 0L)).isFalse();
    }

    @Test public void testInlineImageIsNotEligible() {
        Metadata m = image();
        m.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE,
              TikaCoreProperties.EmbeddedResourceType.INLINE.toString());
        assertThat(EmbedSpawner.isOcrEligible(m, 0L)).isFalse();
    }

    @Test public void testImageBelowMinBytesIsNotEligible() {
        Metadata m = image();
        m.set(Metadata.CONTENT_LENGTH, "100");
        assertThat(EmbedSpawner.isOcrEligible(m, 4096L)).isFalse();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerEligibilityTest`
Expected: FAIL (method does not exist).

- [ ] **Step 3: Write minimal implementation**

Add to `EmbedSpawner` (import `org.apache.tika.metadata.Metadata` is already present):

```java
    static boolean isOcrEligible(final Metadata metadata, final long minImageBytes) {
        final String contentType = metadata.get(Metadata.CONTENT_TYPE);
        if (contentType == null || !contentType.startsWith("image/")) {
            return false;
        }
        final String resourceType = metadata.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE);
        if (TikaCoreProperties.EmbeddedResourceType.INLINE.toString().equals(resourceType)) {
            return false;
        }
        if (minImageBytes > 0) {
            final String length = metadata.get(Metadata.CONTENT_LENGTH);
            if (length != null) {
                try {
                    if (Long.parseLong(length) < minImageBytes) {
                        return false;
                    }
                } catch (final NumberFormatException ignored) { /* treat as unknown -> eligible */ }
            }
        }
        return true;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerEligibilityTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerEligibilityTest.java
git commit -m "feat(ocr): add OCR-eligibility predicate for image-leaf embeds"
```

---

### Task 6: Deferred-OCR wiring in `EmbedSpawner` (digest eager, OCR async)

This is the integration task. `EmbedSpawner` receives the shared OCR executor, the per-file `ExtractionProgress`, the digester, and the `ocrFanout`/`ocrMinImageBytes` flags (passed by `Extractor` from `getTikaDocument`). For an eligible embed it: spools bytes, digests synchronously, submits OCR to the pool, and backs the embed reader with the future. For everything else it calls `spawnEmbedded` exactly as today.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java`
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java` (`getTikaDocument`: construct `EmbedSpawner` with the new dependencies; collect and join OCR futures before spew; `begin`/`end` the tracker around `extract`).
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerDeferredOcrTest.java`

**Interfaces:**
- Consumes: `Extractor.getOcrExecutor()`, `ExtractionProgress`, `EmbedSpawner.isOcrEligible` (Task 5), `BudgetedEmbedBuffer`, `DigestingParser.Digester`.
- Produces:
  - New `EmbedSpawner` constructor parameters appended in order: `ExecutorService ocrExecutor, List<Future<?>> ocrFutures, ExtractionProgress progress, DigestingParser.Digester digester, boolean ocrFanout, long ocrMinImageBytes`.
  - `Extractor`: a parse-scoped `List<Future<?>> ocrFutures` created in `getTikaDocument`, awaited in `doExtract` (or in `extract(Path)`) before `spewer.write`.

**Design notes the implementer MUST honor (from the spec):**
- Build the embed node, name, and stack push/pop synchronously, identical to today, so IDs are unchanged.
- Digest the spooled bytes synchronously and set `X-TIKA:digest:*` on the embed metadata before returning, so `embed.getHash()`, the embed ID, and `writeEmbed` work on the walk thread.
- Submit only the OCR text parse to the pool. Increment `progress.incrementOcrSubmitted()` on submit and `progress.incrementOcrCompleted()` in the task `finally`.
- The embed reader generator must block on the OCR `Future` before yielding text (backstop); `Extractor` also joins all futures before spew.
- On any OCR task exception, record `TIKA_META_EXCEPTION_EMBEDDED_STREAM` on the embed metadata; never propagate as fatal.
- When `ocrFanout` is false OR the embed is not `isOcrEligible(...)`, fall through to the existing synchronous `spawnEmbedded` path unchanged.
- `BudgetedEmbedBuffer.write` is `synchronized`, so concurrent OCR writes to distinct buffers are safe; its shared `reserved` `AtomicLong` is already atomic. Keep one buffer per embed (as today).

- [ ] **Step 1: Write the failing test** (proves eligible image OCR is deferred to the executor and joined; uses an injected single-thread executor and a stub that records the calling thread)

```java
package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.digestutils.CommonsDigester;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerDeferredOcrTest {
    @Test public void testEligibleImageSubmitsOcrToExecutorAndDigestsSynchronously() throws Exception {
        TikaDocument root = new DocumentFactory().create(java.nio.file.Paths.get("/root.eml"));
        ExecutorService ocr = Executors.newSingleThreadExecutor();
        List<Future<?>> futures = new ArrayList<>();
        ExtractionProgress progress = new ExtractionProgress(root.getPath(), 0L);
        DigestingParser.Digester digester = new CommonsDigester(20 * 1024 * 1024, "SHA256");

        try (TemporaryResources tmp = new TemporaryResources()) {
            EmbedSpawner spawner = new EmbedSpawner(
                root, new org.apache.tika.parser.ParseContext(), null,
                writer -> new org.apache.tika.sax.BodyContentHandler(writer),
                64L * 1024 * 1024, tmp, () -> false,
                ocr, futures, progress, digester, /*ocrFanout*/ true, /*ocrMinImageBytes*/ 0L);

            Metadata m = new Metadata();
            m.set(Metadata.CONTENT_TYPE, "image/png");
            m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, "pic.png");

            spawner.parseEmbedded(
                new ByteArrayInputStream("PNG\r\n".getBytes(StandardCharsets.UTF_8)),
                new org.apache.tika.sax.BodyContentHandler(), m, false);

            assertThat(progress.ocrSubmitted()).isEqualTo(1L);
            assertThat(futures).hasSize(1);
            futures.get(0).get();                 // join
            assertThat(progress.ocrCompleted()).isEqualTo(1L);
            // digest set synchronously on the embed metadata
            assertThat(root.getEmbeds().iterator().next().getMetadata()
                .get("X-TIKA:digest:SHA256")).isNotNull();
        } finally {
            ocr.shutdownNow();
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerDeferredOcrTest`
Expected: FAIL (constructor signature does not exist).

- [ ] **Step 3: Write minimal implementation**

In `EmbedSpawner`, add fields and extend the constructor (append the new parameters after the existing ones; keep the old constructor delegating with `ocrExecutor=null, ocrFutures=null, progress=null, digester=null, ocrFanout=false, ocrMinImageBytes=0` for callers/tests that don't fan out):

```java
    private final java.util.concurrent.ExecutorService ocrExecutor;
    private final java.util.List<java.util.concurrent.Future<?>> ocrFutures;
    private final ExtractionProgress progress;
    private final org.apache.tika.parser.DigestingParser.Digester digester;
    private final boolean ocrFanout;
    private final long ocrMinImageBytes;

    EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
                 final Function<Writer, ContentHandler> handlerFunction,
                 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
                 final BooleanSupplier memoryPressureHigh,
                 final java.util.concurrent.ExecutorService ocrExecutor,
                 final java.util.List<java.util.concurrent.Future<?>> ocrFutures,
                 final ExtractionProgress progress,
                 final org.apache.tika.parser.DigestingParser.Digester digester,
                 final boolean ocrFanout, final long ocrMinImageBytes) {
        super(root, context);
        this.outputPath = outputPath;
        this.handlerFunction = handlerFunction;
        this.embedMemoryBudgetBytes = embedMemoryBudgetBytes;
        this.tmp = tmp;
        this.memoryPressureHigh = memoryPressureHigh;
        this.ocrExecutor = ocrExecutor;
        this.ocrFutures = ocrFutures;
        this.progress = progress;
        this.digester = digester;
        this.ocrFanout = ocrFanout;
        this.ocrMinImageBytes = ocrMinImageBytes;
        tikaDocumentStack.add(root);
    }
```

In `parseEmbedded`, in the `else` branch (the non-INLINE branch that currently calls `spawnEmbedded`), bump the embed counter and route eligible images to the deferred path:

```java
        } else {
            if (progress != null) { progress.incrementEmbeds(); }
            if (ocrFanout && ocrExecutor != null && isOcrEligible(metadata, ocrMinImageBytes)) {
                spawnEmbeddedDeferred(TikaInputStream.get(input), metadata);
            } else {
                spawnEmbedded(TikaInputStream.get(input), metadata);
            }
        }
```

Add the deferred path. It mirrors `spawnEmbedded` for node/stack/digest bookkeeping but submits OCR to the pool:

```java
    private void spawnEmbeddedDeferred(final TikaInputStream tis, final Metadata metadata) throws IOException {
        final BudgetedEmbedBuffer buffer =
                new BudgetedEmbedBuffer(reserved, embedMemoryBudgetBytes, tmp, memoryPressureHigh);
        final Writer writer = new OutputStreamWriter(buffer, StandardCharsets.UTF_8);
        final ContentHandler embedHandler = handlerFunction.apply(writer);
        final EmbeddedTikaDocument embed = tikaDocumentStack.getLast().addEmbed(metadata);

        String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
        if (null == name || name.isEmpty()) { name = String.format("untitled_%d", ++untitled); }

        // Spool the bytes now (the stream is only valid during this call).
        final Path spooled = tis.getPath();

        // Digest synchronously so the embed ID and artifact filename are identical to serial mode.
        try (InputStream digestStream = Files.newInputStream(spooled)) {
            digester.digest(digestStream, metadata, context);
        } catch (final Exception e) {
            logger.error("Unable to digest embedded image \"{}\" (in \"{}\").", name, root, e);
        }

        // Back the reader with the OCR future so spew blocks until text is ready.
        final java.util.concurrent.CompletableFuture<Void> done = new java.util.concurrent.CompletableFuture<>();
        embed.setReader(() -> { join(done); return buffer.readerGenerator().generate(); });

        if (null != this.outputPath) { writeEmbed(tis, embed, name); }

        if (progress != null) { progress.incrementOcrSubmitted(); }
        final String embedName = name;
        ocrFutures.add(ocrExecutor.submit(() -> {
            try (InputStream in = Files.newInputStream(spooled)) {
                delegateParsing(TikaInputStream.get(in), embedHandler, metadata);
            } catch (final Throwable t) {
                logger.error("Deferred OCR failed for \"{}\" (in \"{}\").", embedName, root, t);
                metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
                        org.apache.tika.utils.ExceptionUtils.getFilteredStackTrace(t));
            } finally {
                try { writer.close(); } catch (final IOException ignored) {}
                if (progress != null) { progress.incrementOcrCompleted(); }
                done.complete(null);
            }
            return null;
        }));
    }

    private static void join(final java.util.concurrent.Future<Void> f) {
        try { f.get(); }
        catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
        catch (final java.util.concurrent.ExecutionException ignored) {}
    }
```

Add the imports `java.io.InputStream`, `java.nio.file.Files`, `java.nio.file.Path` (Path/Files are already imported via `java.nio.file.*`; `InputStream` via `java.io.*`).

In `Extractor.getTikaDocument`, where the SPAWN `EmbedSpawner` is built (currently lines ~761-763), create a parse-scoped futures list and pass the new dependencies. The `ExtractionProgress` for this file comes from the tracker; pass it down. Replace the `new EmbedSpawner(...)` call with:

```java
            final java.util.List<java.util.concurrent.Future<?>> ocrFutures =
                    new java.util.concurrent.CopyOnWriteArrayList<>();
            context.set(EmbeddedDocumentExtractor.class,
                    new EmbedSpawner(rootDocument, context, embedOutput, handlerProvider, embedMemoryBudgetBytes,
                            embedTextResources, new MemoryPressureGauge(embedMemoryPressureThreshold),
                            ocrExecutor, ocrFutures, currentProgress, digester, ocrFanout, ocrMinImageBytes));
            this.pendingOcrFutures.set(ocrFutures);   // stash for the join in doExtract
```

Thread the per-file `ExtractionProgress` and futures list through `extract(Path)` -> `doExtract`. The simplest scoping: make `getTikaDocument` accept and return them via a small holder, or store them in a `ThreadLocal<List<Future<?>>> pendingOcrFutures` set in `getTikaDocument` and read in `doExtract`. Use the `ThreadLocal` approach to avoid changing public signatures:

```java
    private final ThreadLocal<java.util.List<java.util.concurrent.Future<?>>> pendingOcrFutures =
            ThreadLocal.withInitial(java.util.Collections::emptyList);
```

In `doExtract`, wrap with tracker begin/end and join futures before spew:

```java
    private void doExtract(final Path path, final Spewer spewer) throws IOException {
        long before = currentTimeMillis();
        progressTracker.begin(path);
        try {
            TikaDocument document = extract(path);
            // Wait for all deferred OCR of this file before joining text into the spew.
            for (final java.util.concurrent.Future<?> f : pendingOcrFutures.get()) {
                try { f.get(); }
                catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
                catch (final java.util.concurrent.ExecutionException ignored) {}
            }
            logger.info("{} extracted in {}ms", path, currentTimeMillis() - before);
            spewer.write(document);
        } finally {
            pendingOcrFutures.remove();
            progressTracker.end(path);
        }
    }
```

`currentProgress` in `getTikaDocument`: read the tracker entry for this path, `progressTracker.inFlight()` keyed by path — add a `ExtractionProgressTracker.get(Path)` accessor returning the live `ExtractionProgress`, or pass it via the same `ThreadLocal` pattern. Add to the tracker:

```java
    public ExtractionProgress get(final Path path) { return inFlight.get(path); }
```

and in `getTikaDocument`: `final ExtractionProgress currentProgress = progressTracker.get(path);` (guard null for the OCR-disabled / non-SPAWN callers).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerDeferredOcrTest`
Expected: PASS.

- [ ] **Step 5: Run the full extractor test suite to catch regressions**

Run: `mvn -pl extract-lib test -Dtest=Extractor*,EmbedSpawner*,BudgetedEmbedBuffer*`
Expected: PASS (existing extractor/embed/buffer tests still green).

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgressTracker.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerDeferredOcrTest.java
git commit -m "feat(ocr): defer image-attachment OCR to shared pool, digest eagerly"
```

---

### Task 7: Enriched await message in `DocumentConsumer`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/DocumentConsumer.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/DocumentConsumerAwaitMessageTest.java`

**Interfaces:**
- Consumes: `Extractor.getProgressTracker()`, `ExtractionProgress`.
- Produces: `static String inFlightSummary(Collection<ExtractionProgress> inFlight, long now)` on `DocumentConsumer`, used by the enriched timeout log.

- [ ] **Step 1: Write the failing test**

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=DocumentConsumerAwaitMessageTest`
Expected: FAIL (method does not exist).

- [ ] **Step 3: Write minimal implementation**

Add to `DocumentConsumer`:

```java
    static String inFlightSummary(final java.util.Collection<ExtractionProgress> inFlight, final long now) {
        if (inFlight.isEmpty()) { return "none"; }
        final StringBuilder sb = new StringBuilder();
        for (final ExtractionProgress p : inFlight) {
            if (sb.length() > 0) { sb.append("; "); }
            sb.append(p.path()).append(" (").append(p.elapsedMillis(now) / 1000L).append("s)");
        }
        return sb.toString();
    }
```

Override `awaitTermination` so the timeout path logs the in-flight summary instead of the generic `ExecutorProxy` warning (the consumer holds `extractor`, which exposes the tracker):

```java
    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        final boolean terminated = executor.awaitTermination(timeout, unit);
        if (!terminated) {
            logger.warn(String.format("Still extracting after %d %s; in flight: %s", timeout, unit,
                    inFlightSummary(extractor.getProgressTracker().inFlight(), System.currentTimeMillis())));
        }
        try {
            spewer.close();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return terminated;
    }
```

(Note: this replaces the previous `super.awaitTermination(...)` call. `logger` is already a field on `DocumentConsumer`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=DocumentConsumerAwaitMessageTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/DocumentConsumer.java \
        extract-lib/src/test/java/org/icij/extract/extractor/DocumentConsumerAwaitMessageTest.java
git commit -m "feat(progress): name in-flight files in DocumentConsumer await timeout log"
```

---

### Task 8: Determinism / on-demand contract regression test

Proves the Global Constraint: parallel OCR produces the same document tree and embed IDs as serial OCR, and an embed indexed under parallel OCR is still resolvable by ID through the on-demand path.

**Files:**
- Create test: `extract-lib/src/test/java/org/icij/extract/extractor/ParallelOcrDeterminismTest.java`
- Reuse an existing image-bearing test fixture under `extract-lib/src/test/resources/documents/` (pick one containing an image attachment, e.g. an `.eml`/`.msg`/`.pdf` already used by other tests — verify with `ls extract-lib/src/test/resources/documents`).

**Interfaces:**
- Consumes: `Extractor` (with `ocrParallelism`, `ocrFanout`), `Spewer` (an in-memory collecting spewer, or reuse the existing test spewer used elsewhere in the suite).

- [ ] **Step 1: Identify the fixture and the existing in-memory spewer**

Run: `ls extract-lib/src/test/resources/documents | grep -iE "eml|msg|pdf|png|jpg"`
Run: `grep -rln "extends Spewer\|class .*Spewer" extract-lib/src/test/java | head`
Pick the fixture with an embedded image and the test spewer those tests use. If no collecting test spewer exists, collect IDs by walking `TikaDocument.getEmbeds()` after `extract(path)` instead of spewing.

- [ ] **Step 2: Write the failing test**

```java
package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.task.Options;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

public class ParallelOcrDeterminismTest {
    // TODO at Step 1: set to the chosen image-bearing fixture path.
    private static final Path FIXTURE = Paths.get("extract-lib/src/test/resources/documents/REPLACE_ME");

    private List<String> idsAfterExtract(int parallelism, boolean fanout) throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "ocrParallelism", String.valueOf(parallelism),
                "ocrFanout", String.valueOf(fanout),
                "progressHeartbeatInterval", "0")))) {
            TikaDocument doc = extractor.extract(FIXTURE);
            // Realize readers (deferred OCR) by reading them, then collect ids depth-first.
            List<String> ids = new ArrayList<>();
            collect(doc, ids);
            return ids;
        }
    }

    private void collect(TikaDocument doc, List<String> ids) throws Exception {
        for (EmbeddedTikaDocument e : doc.getEmbeds()) {
            ids.add(e.getId());
            collect(e, ids);
        }
    }

    @Test public void testParallelAndSerialProduceIdenticalEmbedIds() throws Exception {
        List<String> serial = idsAfterExtract(1, false);
        List<String> parallel = idsAfterExtract(8, true);
        assertThat(parallel).isEqualTo(serial);
        assertThat(serial).isNotEmpty();
    }
}
```

- [ ] **Step 3: Run test to verify it fails, then fill the fixture path**

Run: `mvn -pl extract-lib test -Dtest=ParallelOcrDeterminismTest`
Expected: FAIL first on the `REPLACE_ME` path. Replace `FIXTURE` with the real image-bearing fixture from Step 1, then rerun.

- [ ] **Step 4: Make it pass**

If IDs differ, the digest is not being computed identically in the deferred path — revisit Task 6 Step 3 (the synchronous `digester.digest(...)` must run on the same bytes with the same digester). Do not "fix" by changing the assertion. Once aligned:

Run: `mvn -pl extract-lib test -Dtest=ParallelOcrDeterminismTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/test/java/org/icij/extract/extractor/ParallelOcrDeterminismTest.java
git commit -m "test(ocr): assert parallel OCR yields identical embed IDs to serial"
```

---

### Task 9: Full module suite + on-demand round-trip guard

**Files:**
- Modify test (extend Task 8): add a round-trip assertion using `EmbeddedDocumentExtractor.extract` to prove a parallel-OCR-produced embed is resolvable by ID (the `SourceExtractor` path).

**Interfaces:**
- Consumes: `org.icij.extract.extractor.EmbeddedDocumentExtractor` (unchanged on-demand walker), the digester and artifact path conventions.

- [ ] **Step 1: Write the failing assertion** (append to `ParallelOcrDeterminismTest`)

```java
    @Test public void testParallelOcrEmbedResolvableOnDemand() throws Exception {
        // Index with parallel OCR, capture an embed id, then resolve it via the on-demand walker.
        List<String> ids = idsAfterExtract(8, true);
        String anEmbedId = ids.get(0);

        org.apache.tika.parser.DigestingParser.Digester digester =
                new org.apache.tika.parser.digestutils.CommonsDigester(20 * 1024 * 1024, "SHA256");
        EmbeddedDocumentExtractor onDemand =
                new EmbeddedDocumentExtractor(digester, "SHA256", null, /*useOcr*/ true);
        org.icij.extract.document.TikaDocument root =
                new org.icij.extract.document.DocumentFactory()
                    .withIdentifier(new org.icij.extract.document.DigestIdentifier("SHA256", java.nio.charset.Charset.defaultCharset()))
                    .create(FIXTURE);
        // Must not throw ContentNotFoundException.
        assertThat(onDemand.extract(root, anEmbedId)).isNotNull();
    }
```

- [ ] **Step 2: Run to verify it fails then passes**

Run: `mvn -pl extract-lib test -Dtest=ParallelOcrDeterminismTest#testParallelOcrEmbedResolvableOnDemand`
Expected: PASS (resolves by ID). If it throws `ContentNotFoundException`, the bulk-path digest diverged — fix Task 6, not the test. (Check `EmbeddedDocumentExtractor.extract` signature with `grep -n "public .* extract" extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java` and adjust the call to match — useOcr flag and artifact path argument order.)

- [ ] **Step 3: Run the whole module suite**

Run: `mvn -pl extract-lib test`
Expected: PASS. (Some suite tests require a running Redis per the Makefile; start it if needed: `redis-server` / the devenv Redis at `redis:6379`.)

- [ ] **Step 4: Commit**

```bash
git add extract-lib/src/test/java/org/icij/extract/extractor/ParallelOcrDeterminismTest.java
git commit -m "test(ocr): assert parallel-OCR embeds resolve via on-demand extractor"
```

---

## Datashare follow-up (out of scope for this plan, note for later)

After releasing extract-lib, in the `datashare` repo: bump `extract.version`; mirror the four new options in `DatashareCliOptions`; optionally register a `ProgressListener` in `IndexTask` to surface progress in task status; and call `Extractor.close()` per task in server mode so OCR pools do not accumulate.

## Self-Review

**Spec coverage:**
- B1 shared pool + within-file fan-out -> Tasks 4, 6. Cross-file cap is inherent (single shared `ocrExecutor`).
- Digest-eager / OCR-async correctness -> Task 6; asserted by Tasks 8, 9.
- A1 heartbeat + programmatic hook -> Tasks 2, 3, 4 (`addProgressListener`).
- A2 enriched await message -> Task 7.
- Parameters (`ocrParallelism`, `ocrFanout`, `ocrMinImageBytes`, `progressHeartbeatInterval`) -> Task 4; serial-equivalence path -> Tasks 5/6 fall-through, asserted by Task 8 (`ocrFanout=false`).
- On-demand / artifact contract unchanged -> Task 9 round-trip.
- Safety: no-deadlock (leaf-only tasks) honored by eligibility (Task 5) + Task 6; memory bounded via per-embed `BudgetedEmbedBuffer` (unchanged) noted in Task 6.

**Placeholder scan:** The only intentional placeholder is `FIXTURE = REPLACE_ME` in Task 8, resolved in Task 8 Step 1/Step 3 by listing real fixtures (a fixture name cannot be invented; it must be chosen from the repo). Two "confirm the API before Step 4" notes (Options `asLong`, `EmbeddedDocumentExtractor.extract` signature) are verification steps, not deferred work.

**Type consistency:** `ExtractionProgress` counter method names (`incrementEmbeds`/`incrementOcrSubmitted`/`incrementOcrCompleted`, `embedsParsed`/`ocrSubmitted`/`ocrCompleted`) are used identically in Tasks 1, 3, 6, 7. Tracker `begin`/`end`/`inFlight`/`get`/`addListener`/`tick`/`start`/`close` consistent across Tasks 2, 4, 6, 7. `EmbedSpawner` constructor parameter order (…, `ocrExecutor`, `ocrFutures`, `progress`, `digester`, `ocrFanout`, `ocrMinImageBytes`) consistent between Task 6 definition and the `Extractor.getTikaDocument` call site.
