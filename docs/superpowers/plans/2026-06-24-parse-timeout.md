# Wall-clock Parse Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound each document's parse+output with a wall-clock timeout so a hung parser frees its worker slot and is recorded as a failure instead of pinning the thread pool forever.

**Architecture:** `Extractor.extract(Path, Spewer)` runs the existing parse+spew on a shared daemon watchdog executor and blocks on `Future.get(parseTimeout)` — the same pattern as `Tess4JOCRParser.runWithTimeout`. A timeout throws a new `ParseTimeoutException` (an `IOException`) that flows through the existing error-recording path and is recorded as a new `ExtractionStatus.FAILURE_TIMEOUT`. All other parser throwables propagate verbatim, so the existing recoverable-vs-fatal classification is untouched.

**Tech Stack:** Java 17, Apache Tika, JUnit 4, FEST assertions (`org.fest.assertions.Assertions.assertThat`), Maven (modules `extract-lib`, `extract-cli`).

## Global Constraints

- JDK 17; Maven 3.8+ (per README).
- Tests use JUnit 4 (`org.junit.Test`, `@Rule`, `TemporaryFolder`) and FEST assertions (`org.fest.assertions.Assertions.assertThat`).
- Conventional-commit messages; every commit ends with the trailer:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- Default `parseTimeout` is **24h** (`Duration.ofDays(1)`), enabled by default; `0`/negative disables the watchdog.
- New status code value is **11** (`FAILURE_FATAL` is 10; codes are stable integers used in reports).
- Build/test command for the library module: `mvn -q -pl extract-lib test -Dtest=<ClassName>` (full module suite: `mvn -q -pl extract-lib test`). Redis must be running for the full suite; the new tests do not need it.

---

### Task 1: Add `FAILURE_TIMEOUT` status

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionStatus.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionStatusTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `ExtractionStatus.FAILURE_TIMEOUT` with code `11`, resolvable via `ExtractionStatus.parse(11)` and `ExtractionStatus.parse("FAILURE_TIMEOUT")`.

- [ ] **Step 1: Write the failing test**

Add to `ExtractionStatusTest.java`:

```java
@Test
public void testFailureTimeoutCodeRoundTrips() {
    assertThat(ExtractionStatus.FAILURE_TIMEOUT.getCode()).isEqualTo(11);
    assertThat(ExtractionStatus.parse(11)).isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
    assertThat(ExtractionStatus.parse("FAILURE_TIMEOUT")).isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
}
```

If `ExtractionStatusTest` does not already import FEST assertions, add:
`import static org.fest.assertions.Assertions.assertThat;`

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractionStatusTest`
Expected: FAIL — compilation error, `FAILURE_TIMEOUT` does not exist.

- [ ] **Step 3: Add the enum constant**

In `ExtractionStatus.java`, change the constant list so `FAILURE_TIMEOUT(11)` is appended after `FAILURE_FATAL(10)`:

```java
	SUCCESS(0),
	FAILURE_NOT_FOUND(1),
	FAILURE_UNREADABLE(2),
	FAILURE_NOT_DECRYPTED(3),
	FAILURE_NOT_PARSED(4),
	FAILURE_UNKNOWN(8),
	FAILURE_NOT_SAVED(9),
	FAILURE_FATAL(10),
	FAILURE_TIMEOUT(11);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractionStatusTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ExtractionStatus.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionStatusTest.java
git commit -m "feat(extractor): add FAILURE_TIMEOUT extraction status

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Add `ParseTimeoutException`

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/ParseTimeoutException.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ParseTimeoutExceptionTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `public class ParseTimeoutException extends IOException` with constructor `ParseTimeoutException(Path path, Duration timeout)` and accessors `Path getPath()` / `Duration getTimeout()`. Used by Task 3 to signal a timeout and by `Extractor.status(...)` to map to `FAILURE_TIMEOUT`.

- [ ] **Step 1: Write the failing test**

Create `ParseTimeoutExceptionTest.java`:

```java
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

        assertThat(e.getPath()).isEqualTo(path);
        assertThat(e.getTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(e.getMessage()).contains("doc.pdf");
        assertThat(e.getMessage()).contains("PT2M");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ParseTimeoutExceptionTest`
Expected: FAIL — compilation error, `ParseTimeoutException` does not exist.

- [ ] **Step 3: Create the class**

Create `ParseTimeoutException.java`:

```java
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ParseTimeoutExceptionTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ParseTimeoutException.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ParseTimeoutExceptionTest.java
git commit -m "feat(extractor): add ParseTimeoutException

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Wrap the parse path in a wall-clock watchdog

This is the core task. It adds the `parseTimeout` field + option + watchdog executor, splits the current `extract(Path, Spewer)` body into a private `doExtract`, wraps it in `Future.get(timeout)`, and routes a timeout to `FAILURE_TIMEOUT`.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java`
  - Add imports, `parseTimeout` field, watchdog executor field.
  - Add `@Option(name = "parseTimeout", ...)` annotation and `configure(...)` line.
  - Add `setParseTimeout(Duration)`.
  - Refactor `extract(Path, Spewer)` (currently `Extractor.java:354-361`) into a public wrapper + private `doExtract(Path, Spewer)`.
  - Add a `ParseTimeoutException` branch to `status(...)` (`Extractor.java:466-497`).
  - Add a `case FAILURE_TIMEOUT` to `log(...)` (`Extractor.java:431-456`).
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorParseTimeoutTest.java`

**Interfaces:**
- Consumes: `ExtractionStatus.FAILURE_TIMEOUT` (Task 1), `ParseTimeoutException(Path, Duration)` (Task 2).
- Produces:
  - `public void setParseTimeout(Duration parseTimeout)` on `Extractor`.
  - Behavior: `extract(Path, Spewer)` throws `ParseTimeoutException` when parse+spew exceed `parseTimeout`; `extract(Path, Spewer, Reporter)` records `FAILURE_TIMEOUT` for it.
  - The `parseTimeout` option key `"parseTimeout"` (duration string, default `"24h"`, `0` disables).

- [ ] **Step 1: Write the failing test**

Create `ExtractorParseTimeoutTest.java`. The test injects a **blocking reader** by overriding the lazy `extract(Path)` so the read inside `spewer.write` hangs, which is exactly what the watchdog must interrupt.

```java
package org.icij.extract.extractor;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.report.HashMapReportMap;
import org.icij.extract.report.Report;
import org.icij.extract.report.Reporter;
import org.icij.spewer.FieldNames;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractorParseTimeoutTest {

    /** Never releases on its own — a deterministic hang that only an interrupt can end. */
    private final CountDownLatch latch = new CountDownLatch(1);

    @After
    public void releaseLatch() {
        latch.countDown();
    }

    /** A Reader whose read() blocks until interrupted (or the latch is released in teardown). */
    private class BlockingReader extends Reader {
        @Override
        public int read(final char[] cbuf, final int off, final int len) throws IOException {
            try {
                latch.await();
                return -1;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
        }

        @Override
        public void close() {
        }
    }

    /** Extractor whose lazy extract(Path) returns a document backed by a hanging reader. */
    private class HangingExtractor extends Extractor {
        @Override
        public TikaDocument extract(final Path path) {
            final TikaDocument document =
                    new DocumentFactory().withIdentifier(new PathIdentifier()).create(path);
            document.setReader(new BlockingReader());
            return document;
        }
    }

    private Spewer nullSpewer() {
        return new PrintStreamSpewer(
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8), new FieldNames());
    }

    @Test(timeout = 10_000)
    public void testHungParseRecordsFailureTimeoutAndReturns() {
        final HashMapReportMap reportMap = new HashMapReportMap();
        final Reporter reporter = new Reporter(reportMap);
        final Path path = Paths.get("hang");

        final Extractor extractor = new HangingExtractor();
        extractor.setParseTimeout(Duration.ofMillis(200));

        // Must return (slot freed), not throw, not hang.
        extractor.extract(path, nullSpewer(), reporter);

        final Report report = reportMap.get(path);
        assertThat(report).isNotNull();
        assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorParseTimeoutTest`
Expected: FAIL — `setParseTimeout` does not exist (compilation error). (Before the watchdog exists, the call would also hang, which is why the test asserts via `@Test(timeout = ...)`.)

- [ ] **Step 3: Add imports, fields, option, and setter**

In `Extractor.java`, add to the imports block:

```java
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
```

Add the `@Option` annotation alongside the other class-level `@Option` annotations (just below the `ocrTimeout` one):

```java
@Option(name = "parseTimeout", description = "Wall-clock timeout for a single document's parse " +
        "and output, e.g. \"30m\" or \"24h\". Set to 0 to disable. Defaults to 24h.", parameter = "duration")
```

Add fields near `embedMemoryPressureThreshold` (around `Extractor.java:140`):

```java
    private Duration parseTimeout = Duration.ofDays(1);
    private final ExecutorService parseExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger();
        @Override
        public Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r, "extract-watchdog-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    });
```

Add the option wiring inside `configure(...)`, next to the `ocrTimeout` line (`Extractor.java:190`):

```java
        options.get("parseTimeout", "24h").parse().asDuration().ifPresent(this::setParseTimeout);
```

Add the setter near the other setters (e.g. after `setEmbedMemoryPressureThreshold`):

```java
    /**
     * Set the wall-clock timeout for a single document's parse and output.
     * A zero or negative duration disables the watchdog.
     *
     * @param parseTimeout the timeout duration
     */
    public void setParseTimeout(final Duration parseTimeout) {
        this.parseTimeout = parseTimeout;
    }
```

- [ ] **Step 4: Refactor `extract(Path, Spewer)` into a watchdog wrapper + `doExtract`**

Replace the current method (`Extractor.java:354-361`):

```java
    public void extract(final Path path, final Spewer spewer) throws IOException {
        long before = currentTimeMillis();
        TikaDocument document = extract(path);
        logger.info("{} extracted in {}ms", path, currentTimeMillis() - before);
        // Spewer.write() walks the whole tree and closes every document reader (including the
        // root, which releases any spilled embed-text temp files), so no cleanup is needed here.
        spewer.write(document);
    }
```

with:

```java
    public void extract(final Path path, final Spewer spewer) throws IOException {
        if (parseTimeout == null || parseTimeout.isZero() || parseTimeout.isNegative()) {
            doExtract(path, spewer);
            return;
        }

        final Future<?> future = parseExecutor.submit(() -> {
            doExtract(path, spewer);
            return null;
        });

        try {
            future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            // Interrupt the worker: it unblocks any pipe read, Spewer.write's finally closes the
            // reader, and the background parse thread stops on its next write. A parser in a tight
            // CPU loop that never touches the pipe cannot be killed and leaks until the next restart.
            future.cancel(true);
            throw new ParseTimeoutException(path, parseTimeout);
        } catch (final ExecutionException e) {
            // Propagate the worker's original throwable verbatim so the existing recoverable-vs-fatal
            // classification (and the fatal -> process-exit path) is unchanged.
            sneakyThrow(e.getCause());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new IOException("Extraction interrupted: " + path, e);
        }
    }

    private void doExtract(final Path path, final Spewer spewer) throws IOException {
        long before = currentTimeMillis();
        TikaDocument document = extract(path);
        logger.info("{} extracted in {}ms", path, currentTimeMillis() - before);
        // Spewer.write() walks the whole tree and closes every document reader (including the
        // root, which releases any spilled embed-text temp files), so no cleanup is needed here.
        spewer.write(document);
    }
```

Note: `sneakyThrow` already exists in this file (`Extractor.java:426-429`).

- [ ] **Step 5: Map the timeout to `FAILURE_TIMEOUT` in `status(...)`**

In `status(final Exception e, final Spewer spewer)` (`Extractor.java:466`), add this as the **first** check inside the method, before the `TaggedIOException` checks:

```java
        if (e instanceof ParseTimeoutException) {
            return ExtractionStatus.FAILURE_TIMEOUT;
        }
```

- [ ] **Step 6: Add a `FAILURE_TIMEOUT` log case**

In `log(final Exception e, final ExtractionStatus status, final Path file)` (`Extractor.java:431`), add a case alongside the others:

```java
            case FAILURE_TIMEOUT:
                logger.error(String.format("Parse exceeded timeout: \"%s\".", file), e);
                break;
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorParseTimeoutTest`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorParseTimeoutTest.java
git commit -m "feat(extractor): wall-clock watchdog on the general parse path

Wrap extract(Path, Spewer) in a Future.get(parseTimeout) on a shared daemon
executor, mirroring the OCR runWithTimeout pattern. A timeout throws
ParseTimeoutException and is recorded as FAILURE_TIMEOUT; all other parser
throwables propagate verbatim. Defaults to 24h; --parseTimeout 0 disables.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Verify the disable path and regression-test verbatim propagation

Adds two more tests to `ExtractorParseTimeoutTest` to lock in the disable behavior and confirm the watchdog does not alter normal exception handling.

**Files:**
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorParseTimeoutTest.java`

**Interfaces:**
- Consumes: `Extractor.setParseTimeout(Duration)` and the timeout behavior from Task 3.
- Produces: nothing (tests only).

- [ ] **Step 1: Write the failing tests**

Add to `ExtractorParseTimeoutTest.java`. The first confirms `parseTimeout = 0` bypasses the watchdog (a real, fast document extracts normally); the second confirms a normal `IOException` from the parse path is still classified exactly as before (regression on verbatim propagation).

Add these imports to the test file:

```java
import org.icij.extract.document.DocumentFactory;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.nio.file.Files;
```

(If an import is already present from Task 3, do not duplicate it.)

Add the tests:

```java
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test(timeout = 10_000)
    public void testDisabledTimeoutExtractsNormally() throws IOException {
        final Path file = folder.newFile("hello.txt").toPath();
        Files.write(file, "hello world".getBytes(StandardCharsets.UTF_8));

        final HashMapReportMap reportMap = new HashMapReportMap();
        final Reporter reporter = new Reporter(reportMap);

        final Extractor extractor = new Extractor();
        extractor.setParseTimeout(Duration.ZERO);   // disabled -> runs doExtract directly

        extractor.extract(file, nullSpewer(), reporter);

        final Report report = reportMap.get(file);
        assertThat(report).isNotNull();
        assertThat(report.getStatus()).isEqualTo(ExtractionStatus.SUCCESS);
    }

    /** Extractor whose lazy extract(Path) throws a plain IOException from the parse path. */
    private static class IOExceptionExtractor extends Extractor {
        @Override
        public TikaDocument extract(final Path path) throws IOException {
            throw new IOException("boom");
        }
    }

    @Test(timeout = 10_000)
    public void testNormalIOExceptionStillClassifiedAsUnreadable() {
        final HashMapReportMap reportMap = new HashMapReportMap();
        final Reporter reporter = new Reporter(reportMap);
        final Path path = Paths.get("broken");

        final Extractor extractor = new IOExceptionExtractor();
        extractor.setParseTimeout(Duration.ofMinutes(5));   // watchdog ON, but no timeout fires

        extractor.extract(path, nullSpewer(), reporter);

        final Report report = reportMap.get(path);
        assertThat(report).isNotNull();
        // Plain IOException with no TikaException cause -> FAILURE_UNREADABLE, same as before the watchdog.
        assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_UNREADABLE);
    }
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorParseTimeoutTest`
Expected: PASS (all tests in the class, including the Task 3 test).

- [ ] **Step 3: Run the existing extractor + error-boundary tests for regressions**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorTest+ExtractorErrorBoundaryTest+ExtractionStatusTest`
Expected: PASS. (The default 24h watchdog now wraps every `extract(Path, Spewer)` call; these confirm behavior is transparent and that fatal/recoverable classification is unchanged.)

- [ ] **Step 4: Commit**

```bash
git add extract-lib/src/test/java/org/icij/extract/extractor/ExtractorParseTimeoutTest.java
git commit -m "test(extractor): cover parseTimeout disable path and verbatim propagation

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Document the option in the README

**Files:**
- Modify: `README.md` (only if it enumerates extraction options; otherwise skip this task and note it in the commit log).

**Interfaces:**
- Consumes: the `parseTimeout` option from Task 3.
- Produces: nothing.

- [ ] **Step 1: Check whether the README documents options**

Run: `grep -n "ocrTimeout" README.md`
Expected: either a line to mirror, or no match.

- [ ] **Step 2: Mirror the `ocrTimeout` documentation**

If `ocrTimeout` is documented in `README.md`, add an adjacent entry for `parseTimeout`, matching the surrounding format, with text: "Wall-clock timeout for a single document's parse and output (e.g. `30m`, `24h`). `0` disables. Defaults to 24h." If `ocrTimeout` is not documented in the README, skip — the `@Option` description is the source of truth and is surfaced by `--help`.

- [ ] **Step 3: Commit (only if the README changed)**

```bash
git add README.md
git commit -m "docs: document the parseTimeout option

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- New `--parseTimeout` option, 24h default, `0` disables → Task 3 (option + field + setter), Task 5 (docs).
- Watchdog via `Future.get` on a shared daemon executor wrapping parse+spew → Task 3.
- Timeout → `ParseTimeoutException` (an `IOException`) → Task 2, Task 3.
- Recorded as new `FAILURE_TIMEOUT(11)` via `status()`/`log()` → Task 1, Task 3 (steps 5–6).
- Verbatim propagation of all other throwables (recoverable + fatal unchanged) → Task 3 (step 4, `ExecutionException` → `sneakyThrow`), regression-tested in Task 4.
- Record + continue (no fatal propagation for timeout) → Task 3 test asserts the call returns and records.
- Disable path → Task 4.
- Leak semantics: accepted, not code — no task needed; the test interrupts the blocked reader, exercising the common recoverable case.

**Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to Task N" — all code is shown in full. Task 5 has a genuine conditional (mirror vs skip) with an explicit decision rule, not a placeholder.

**Type consistency:** `setParseTimeout(Duration)`, `ParseTimeoutException(Path, Duration)`, `ExtractionStatus.FAILURE_TIMEOUT`, option key `"parseTimeout"`, and method `doExtract(Path, Spewer)` are named identically across all tasks. `extract(Path)` is the method overridden by the test doubles, matching the real signature `public TikaDocument extract(final Path path) throws IOException`.
