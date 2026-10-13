# Throwable Boundary for the Extraction Worker — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure every document gets a recorded terminal status even when a parser throws an `Error`, survive recoverable errors, and restart the process clean on `OutOfMemoryError`.

**Architecture:** A single shared predicate `isFatal(Throwable)` classifies errors. `Extractor.extract(…, Reporter)` catches `Throwable`, records recoverable errors as a new `FAILURE_FATAL` status and continues, and best-effort-records then rethrows fatal errors (OOM). `DocumentConsumer.accept` swallows recoverable errors and rethrows fatal ones. `extract-cli`'s `Main` installs a default uncaught-exception handler that exits the JVM on fatal errors so an orchestrator restarts it.

**Tech Stack:** Java 17, Maven (modules `extract-lib`, `extract-cli`), JUnit 4, FEST assertions, SLF4J, Apache Tika.

## Global Constraints

- Java 17; follow existing tab-indentation and brace style of each file being edited (`extract-lib` uses 4-space indent in `extractor/`; `Main.java`/`DocumentConsumer.java` use tabs — match the file you edit).
- `extract-lib` MUST NOT call `System.exit`. Process lifecycle lives in `extract-cli`.
- `Reporter`, `Report`, and the journal are typed on `Exception` — do not widen them to `Throwable`; wrap instead.
- Recoverable vs. fatal classification has exactly one definition: `ExtractionErrors.isFatal`. The CLI reuses it; do not duplicate the logic.
- Fatal = `OutOfMemoryError` and any `VirtualMachineError` that is not `StackOverflowError`. Recoverable = everything else (incl. `StackOverflowError`, `LinkageError`, `AssertionError`, all `Exception`s).
- New report status code: `FAILURE_FATAL(10)`. CLI fatal exit code: `70`.
- Run a single unit test with: `mvn -pl extract-lib test -Dtest=ClassName#method` (these tests need no Redis). For `extract-cli`: `mvn -pl extract-cli -am test -Dtest=ClassName#method`.

---

### Task 1: Add `FAILURE_FATAL` extraction status

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionStatus.java:11-18`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionStatusTest.java` (create)

**Interfaces:**
- Produces: `ExtractionStatus.FAILURE_FATAL` with `getCode() == 10`; `ExtractionStatus.parse(10) == FAILURE_FATAL`.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionStatusTest.java`:

```java
package org.icij.extract.extractor;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractionStatusTest {

	@Test
	public void testFailureFatalHasCodeTen() {
		assertThat(ExtractionStatus.FAILURE_FATAL.getCode()).isEqualTo(10);
	}

	@Test
	public void testParseFailureFatalByCode() {
		assertThat(ExtractionStatus.parse(10)).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}

	@Test
	public void testParseFailureFatalByName() {
		assertThat(ExtractionStatus.parse("FAILURE_FATAL")).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ExtractionStatusTest`
Expected: FAIL — compilation error, `FAILURE_FATAL` does not exist.

- [ ] **Step 3: Add the enum constant**

In `ExtractionStatus.java`, change the constant list (lines 12-18) so the final entry reads:

```java
	SUCCESS(0),
	FAILURE_NOT_FOUND(1),
	FAILURE_UNREADABLE(2),
	FAILURE_NOT_DECRYPTED(3),
	FAILURE_NOT_PARSED(4),
	FAILURE_UNKNOWN(8),
	FAILURE_NOT_SAVED(9),
	FAILURE_FATAL(10);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=ExtractionStatusTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ExtractionStatus.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionStatusTest.java
git commit -m "feat(extractor): add FAILURE_FATAL extraction status"
```

---

### Task 2: `ExtractionError` wrapper

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionError.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionErrorTest.java` (create)

**Interfaces:**
- Produces: `class ExtractionError extends Exception` with constructor `ExtractionError(Throwable cause)`; `getCause()` returns the wrapped throwable; message is the cause's `toString()`.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionErrorTest.java`:

```java
package org.icij.extract.extractor;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractionErrorTest {

	@Test
	public void testPreservesCause() {
		final OutOfMemoryError oom = new OutOfMemoryError("heap");
		final ExtractionError error = new ExtractionError(oom);
		assertThat(error.getCause()).isSameAs(oom);
	}

	@Test
	public void testIsAnException() {
		assertThat((Object) new ExtractionError(new StackOverflowError())).isInstanceOf(Exception.class);
	}

	@Test
	public void testMessageDescribesCause() {
		final ExtractionError error = new ExtractionError(new StackOverflowError());
		assertThat(error.getMessage()).contains("StackOverflowError");
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ExtractionErrorTest`
Expected: FAIL — compilation error, `ExtractionError` does not exist.

- [ ] **Step 3: Create the class**

Create `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionError.java`:

```java
package org.icij.extract.extractor;

/**
 * Wraps a {@link Throwable} (typically an {@link Error}) thrown during extraction so it can be carried through APIs
 * typed on {@link Exception}, such as {@link org.icij.extract.report.Reporter#save}.
 */
public class ExtractionError extends Exception {

	public ExtractionError(final Throwable cause) {
		super(String.valueOf(cause), cause);
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=ExtractionErrorTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ExtractionError.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionErrorTest.java
git commit -m "feat(extractor): add ExtractionError wrapper for non-Exception throwables"
```

---

### Task 3: `ExtractionErrors` classification helpers

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionErrors.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionErrorsTest.java` (create)

**Interfaces:**
- Consumes: `ExtractionError` (Task 2).
- Produces:
  - `static boolean ExtractionErrors.isFatal(Throwable t)` — `true` for `OutOfMemoryError` and `VirtualMachineError` other than `StackOverflowError`; `false` otherwise.
  - `static Exception ExtractionErrors.asException(Throwable t)` — returns `t` cast if it is already an `Exception`, else `new ExtractionError(t)`.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionErrorsTest.java`:

```java
package org.icij.extract.extractor;

import org.junit.Test;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractionErrorsTest {

	@Test
	public void testOutOfMemoryIsFatal() {
		assertThat(ExtractionErrors.isFatal(new OutOfMemoryError())).isTrue();
	}

	@Test
	public void testInternalErrorIsFatal() {
		assertThat(ExtractionErrors.isFatal(new InternalError())).isTrue();
	}

	@Test
	public void testStackOverflowIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new StackOverflowError())).isFalse();
	}

	@Test
	public void testLinkageErrorIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new LinkageError())).isFalse();
	}

	@Test
	public void testRuntimeExceptionIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new RuntimeException())).isFalse();
	}

	@Test
	public void testCheckedExceptionIsNotFatal() {
		assertThat(ExtractionErrors.isFatal(new IOException())).isFalse();
	}

	@Test
	public void testAsExceptionPassesExceptionThrough() {
		final IOException io = new IOException("io");
		assertThat(ExtractionErrors.asException(io)).isSameAs(io);
	}

	@Test
	public void testAsExceptionWrapsError() {
		final StackOverflowError soe = new StackOverflowError();
		final Exception wrapped = ExtractionErrors.asException(soe);
		assertThat(wrapped).isInstanceOf(ExtractionError.class);
		assertThat(wrapped.getCause()).isSameAs(soe);
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ExtractionErrorsTest`
Expected: FAIL — compilation error, `ExtractionErrors` does not exist.

- [ ] **Step 3: Create the class**

Create `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionErrors.java`:

```java
package org.icij.extract.extractor;

/**
 * Classification helpers for throwables raised during extraction.
 *
 * <p>A <em>fatal</em> error leaves the JVM in an uncertain state (notably {@link OutOfMemoryError}); the process
 * should record the failure best-effort and then exit so it can be restarted clean. A <em>recoverable</em> error
 * (such as a parser's {@link StackOverflowError}) affects only the current document and processing can continue.
 */
public final class ExtractionErrors {

	private ExtractionErrors() {}

	/**
	 * @param t the throwable to classify
	 * @return {@code true} for {@link OutOfMemoryError} and any {@link VirtualMachineError} other than
	 *         {@link StackOverflowError}; {@code false} otherwise.
	 */
	public static boolean isFatal(final Throwable t) {
		return t instanceof OutOfMemoryError
				|| (t instanceof VirtualMachineError && !(t instanceof StackOverflowError));
	}

	/**
	 * Adapt a throwable to {@link Exception} so it can be passed to APIs typed on {@code Exception}.
	 *
	 * @param t the throwable to adapt
	 * @return {@code t} itself when it is already an {@link Exception}, otherwise an {@link ExtractionError}
	 *         wrapping it.
	 */
	public static Exception asException(final Throwable t) {
		return (t instanceof Exception) ? (Exception) t : new ExtractionError(t);
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=ExtractionErrorsTest`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ExtractionErrors.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionErrorsTest.java
git commit -m "feat(extractor): add ExtractionErrors fatal/recoverable classification"
```

---

### Task 4: `Extractor.extract(…, Reporter)` Throwable boundary

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java:375-432` (the `extract(Path, Spewer, Reporter)` method and the `log` method)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorErrorBoundaryTest.java` (create)

**Interfaces:**
- Consumes: `ExtractionErrors.isFatal`, `ExtractionErrors.asException` (Task 3); `ExtractionStatus.FAILURE_FATAL` (Task 1); existing `Extractor.extract(Path, Spewer)` (line 354, overridable), `status(Exception, Spewer)` (line 434), `log(Exception, ExtractionStatus, Path)` (line 402), `Reporter.save(Path, ExtractionStatus, Exception)`.
- Produces: behavior — recoverable `Error` → records `FAILURE_FATAL`, returns normally; fatal `Error` → records `FAILURE_FATAL` best-effort, then rethrows the original throwable; `Exception` → unchanged status mapping; a reporter that throws on the fatal path does not mask the original error.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorErrorBoundaryTest.java`. It subclasses `Extractor` to override the parsing overload (`extract(Path, Spewer)`) and throw a chosen throwable, then drives the reporter-aware overload.

```java
package org.icij.extract.extractor;

import org.icij.extract.report.HashMapReportMap;
import org.icij.extract.report.Report;
import org.icij.extract.report.Reporter;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractorErrorBoundaryTest {

	/** An Extractor whose parsing step throws a chosen throwable. */
	private static class ThrowingExtractor extends Extractor {
		private final Throwable toThrow;

		ThrowingExtractor(final Throwable toThrow) {
			this.toThrow = toThrow;
		}

		@Override
		public void extract(final Path path, final Spewer spewer) throws IOException {
			if (toThrow instanceof IOException) {
				throw (IOException) toThrow;
			}
			if (toThrow instanceof RuntimeException) {
				throw (RuntimeException) toThrow;
			}
			if (toThrow instanceof Error) {
				throw (Error) toThrow;
			}
			throw new IllegalStateException("unexpected throwable in test", toThrow);
		}
	}

	private Spewer nullSpewer() {
		return new PrintStreamSpewer(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
				new org.icij.spewer.FieldNames());
	}

	@Test
	public void testRecoverableErrorRecordsFailureFatalAndDoesNotThrow() {
		final HashMapReportMap reportMap = new HashMapReportMap();
		final Reporter reporter = new Reporter(reportMap);
		final Path path = Paths.get("recoverable");

		new ThrowingExtractor(new StackOverflowError()).extract(path, nullSpewer(), reporter);

		final Report report = reportMap.get(path);
		assertThat(report).isNotNull();
		assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}

	@Test
	public void testFatalErrorRecordsFailureFatalThenRethrows() {
		final HashMapReportMap reportMap = new HashMapReportMap();
		final Reporter reporter = new Reporter(reportMap);
		final Path path = Paths.get("fatal");
		final OutOfMemoryError oom = new OutOfMemoryError("synthetic");

		Throwable caught = null;
		try {
			new ThrowingExtractor(oom).extract(path, nullSpewer(), reporter);
		} catch (final Throwable t) {
			caught = t;
		}

		assertThat(caught).isSameAs(oom);
		final Report report = reportMap.get(path);
		assertThat(report).isNotNull();
		assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}

	@Test
	public void testRecordingFailureDoesNotMaskFatalError() {
		final OutOfMemoryError oom = new OutOfMemoryError("synthetic");
		// A reporter whose save throws — simulates inability to record under memory pressure.
		final Reporter reporter = new Reporter(new HashMapReportMap()) {
			@Override
			public void save(final Path path, final ExtractionStatus status, final Exception exception) {
				throw new RuntimeException("cannot record");
			}
		};

		Throwable caught = null;
		try {
			new ThrowingExtractor(oom).extract(Paths.get("fatal"), nullSpewer(), reporter);
		} catch (final Throwable t) {
			caught = t;
		}

		assertThat(caught).isSameAs(oom);
	}

	@Test
	public void testCheckedExceptionStillRecordsNonFatalStatus() {
		final HashMapReportMap reportMap = new HashMapReportMap();
		final Reporter reporter = new Reporter(reportMap);
		final Path path = Paths.get("checked");

		new ThrowingExtractor(new IOException("boom")).extract(path, nullSpewer(), reporter);

		final Report report = reportMap.get(path);
		assertThat(report).isNotNull();
		assertThat(report.getStatus()).isNotEqualTo(ExtractionStatus.SUCCESS);
		assertThat(report.getStatus()).isNotEqualTo(ExtractionStatus.FAILURE_FATAL);
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ExtractorErrorBoundaryTest`
Expected: FAIL — `testFatalErrorRecordsFailureFatalThenRethrows` and `testRecordingFailureDoesNotMaskFatalError` fail because the current `catch (Exception e)` does not catch `Error`, so the OOM escapes before `reporter.save` and (for the masking test) the assertion on the rethrown instance differs. `testRecoverableErrorRecordsFailureFatalAndDoesNotThrow` fails because `StackOverflowError` is not caught.

- [ ] **Step 3: Rewrite the `extract(Path, Spewer, Reporter)` method**

Replace the body of `extract(Path, Spewer, Reporter)` (lines 375-400) so it catches `Throwable`. The current method is:

```java
    public void extract(final Path path, final Spewer spewer, final Reporter reporter) {
        Objects.requireNonNull(reporter);

        if (reporter.skip(path)) {
            logger.info(String.format("File already extracted; skipping: \"%s\".", path));
            return;
        }

        ExtractionStatus status = ExtractionStatus.SUCCESS;
        Exception exception = null;

        try {
            extract(path, spewer);
        } catch (final Exception e) {
            status = status(e, spewer);
            log(e, status, path);
            exception = e;
        }

        // For tagged IO exceptions, discard the tag, which is either unwanted or not serializable.
        if ((exception instanceof TaggedIOException)) {
            exception = ((TaggedIOException) exception).getCause();
        }

        reporter.save(path, status, exception);
    }
```

Replace it with:

```java
    public void extract(final Path path, final Spewer spewer, final Reporter reporter) {
        Objects.requireNonNull(reporter);

        if (reporter.skip(path)) {
            logger.info(String.format("File already extracted; skipping: \"%s\".", path));
            return;
        }

        ExtractionStatus status = ExtractionStatus.SUCCESS;
        Exception exception = null;
        Throwable fatal = null;

        try {
            extract(path, spewer);
        } catch (final Throwable t) {
            if (t instanceof Exception) {
                exception = (Exception) t;
                status = status(exception, spewer);
                log(exception, status, path);
            } else {
                // An Error escaped a parser (e.g. StackOverflowError, OutOfMemoryError).
                status = ExtractionStatus.FAILURE_FATAL;
                exception = ExtractionErrors.asException(t);
                log(exception, status, path);
                if (ExtractionErrors.isFatal(t)) {
                    fatal = t;
                }
            }
        }

        // For tagged IO exceptions, discard the tag, which is either unwanted or not serializable.
        if ((exception instanceof TaggedIOException)) {
            exception = ((TaggedIOException) exception).getCause();
        }

        // Record best-effort. A fatal error may leave the JVM unable to record; never let that mask the original.
        try {
            reporter.save(path, status, exception);
        } catch (final Throwable recordingFailure) {
            if (fatal == null) {
                throw recordingFailure;
            }
            logger.error(String.format("Failed to record status for \"%s\".", path), recordingFailure);
        }

        if (fatal != null) {
            // Propagate so the worker thread's uncaught-exception handler can exit the process for a clean restart.
            sneakyThrow(fatal);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(final Throwable t) throws T {
        throw (T) t;
    }
```

Add the import near the other `java.util` imports (the file already imports `java.util.Objects`; confirm it is present — if not, add `import java.util.Objects;`). `ExtractionErrors` and `ExtractionStatus` are in the same package, so no import is needed.

> Note on `sneakyThrow`: `fatal` is a `Throwable` (an `Error` subtype here), and the method signature does not declare checked throwables. `sneakyThrow` rethrows the original instance without wrapping, preserving the exact `OutOfMemoryError` for the uncaught handler and the test's identity assertion.

- [ ] **Step 4: Extend the `log` switch for `FAILURE_FATAL`**

In the `log(Exception, ExtractionStatus, Path)` method (lines 402-424), add a case before `default`:

```java
            case FAILURE_FATAL:
                logger.error(String.format("A parser threw a fatal error: \"%s\".", file), e);
                break;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=ExtractorErrorBoundaryTest`
Expected: PASS (4 tests).

- [ ] **Step 6: Run the full Extractor test class for regressions**

Run: `mvn -pl extract-lib test -Dtest=ExtractorTest`
Expected: PASS (no regressions in existing extraction behavior). If any test needs Redis it will be skipped/assumed; the existing suite already accounts for that.

- [ ] **Step 7: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorErrorBoundaryTest.java
git commit -m "fix(extractor): record parser Errors as FAILURE_FATAL and rethrow fatal ones"
```

---

### Task 5: `DocumentConsumer.accept` Throwable boundary

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/DocumentConsumer.java` (the `accept` method, the lambda body submitted to `executor.execute`)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/DocumentConsumerErrorTest.java` (create)

**Interfaces:**
- Consumes: `ExtractionErrors.isFatal` (Task 3).
- Produces: behavior — a recoverable throwable from `extractor.extract` is logged and swallowed (worker survives); a fatal throwable is rethrown out of the submitted task (reaches the thread's uncaught-exception handler).

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/DocumentConsumerErrorTest.java`. It runs the task body on a single-thread executor with a custom uncaught-exception handler to observe propagation.

```java
package org.icij.extract.extractor;

import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.fest.assertions.Assertions.assertThat;

public class DocumentConsumerErrorTest {

	/** An Extractor whose reporter-less extract throws a chosen throwable. */
	private static class ThrowingExtractor extends Extractor {
		private final Throwable toThrow;

		ThrowingExtractor(final Throwable toThrow) {
			this.toThrow = toThrow;
		}

		@Override
		public void extract(final Path path, final Spewer spewer) throws IOException {
			if (toThrow instanceof Error) {
				throw (Error) toThrow;
			}
			if (toThrow instanceof RuntimeException) {
				throw (RuntimeException) toThrow;
			}
			throw new IllegalStateException(toThrow);
		}
	}

	private Spewer nullSpewer() {
		return new PrintStreamSpewer(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
				new org.icij.spewer.FieldNames());
	}

	@Test
	public void testRecoverableErrorIsSwallowed() throws Exception {
		final AtomicReference<Throwable> uncaught = new AtomicReference<>();
		final java.util.concurrent.ExecutorService executor = newCapturingExecutor(uncaught);
		final DocumentConsumer consumer =
				new DocumentConsumer(nullSpewer(), new ThrowingExtractor(new StackOverflowError()), executor);

		consumer.accept(Paths.get("recoverable"));
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);

		assertThat(uncaught.get()).isNull();
	}

	@Test
	public void testFatalErrorPropagatesToUncaughtHandler() throws Exception {
		final AtomicReference<Throwable> uncaught = new AtomicReference<>();
		final java.util.concurrent.ExecutorService executor = newCapturingExecutor(uncaught);
		final OutOfMemoryError oom = new OutOfMemoryError("synthetic");
		final DocumentConsumer consumer =
				new DocumentConsumer(nullSpewer(), new ThrowingExtractor(oom), executor);

		consumer.accept(Paths.get("fatal"));
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);

		assertThat(uncaught.get()).isSameAs(oom);
	}

	private java.util.concurrent.ExecutorService newCapturingExecutor(final AtomicReference<Throwable> sink) {
		final ThreadFactory factory = runnable -> {
			final Thread thread = new Thread(runnable);
			thread.setUncaughtExceptionHandler((t, e) -> sink.set(e));
			return thread;
		};
		return java.util.concurrent.Executors.newSingleThreadExecutor(factory);
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=DocumentConsumerErrorTest`
Expected: FAIL — `testFatalErrorPropagatesToUncaughtHandler` fails: the current `catch (Exception e)` does not catch `Error`, so the OOM already reaches the handler (this one may pass), but `testRecoverableErrorIsSwallowed` fails because `StackOverflowError` (an `Error`) is currently NOT caught, so it reaches the uncaught handler instead of being swallowed.

- [ ] **Step 3: Update the `accept` lambda**

In `DocumentConsumer.accept`, the current submitted task is:

```java
		executor.execute(()-> {
			logger.info(String.format("Beginning extraction: \"%s\".", path));

			try {
				if (null != reporter) {
					extractor.extract(path, spewer, reporter);
				} else {
					extractor.extract(path, spewer);
				}
			} catch (Exception e) {
				logger.error(String.format("Exception while consuming file: \"%s\".", path), e);
			}
		});
```

Replace the `catch` block:

```java
		executor.execute(()-> {
			logger.info(String.format("Beginning extraction: \"%s\".", path));

			try {
				if (null != reporter) {
					extractor.extract(path, spewer, reporter);
				} else {
					extractor.extract(path, spewer);
				}
			} catch (Throwable t) {
				logger.error(String.format("Error while consuming file: \"%s\".", path), t);
				if (ExtractionErrors.isFatal(t)) {
					throw t;
				}
			}
		});
```

`ExtractionErrors` is in the same package — no import needed. `throw t;` is legal here because the lambda is a `Runnable` whose body may throw unchecked throwables (`Error`s are unchecked); the rethrown fatal `Error` propagates to the worker thread's uncaught-exception handler.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=DocumentConsumerErrorTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/DocumentConsumer.java \
        extract-lib/src/test/java/org/icij/extract/extractor/DocumentConsumerErrorTest.java
git commit -m "fix(extractor): rethrow fatal errors, swallow recoverable ones in DocumentConsumer"
```

---

### Task 6: CLI default uncaught-exception handler exits on fatal errors

**Files:**
- Modify: `extract-cli/src/main/java/org/icij/extract/cli/Main.java` (the `main` method, top)
- Test: `extract-cli/src/test/java/org/icij/extract/cli/FatalErrorHandlerTest.java` (create)

**Interfaces:**
- Consumes: `ExtractionErrors.isFatal` (Task 3, available because `extract-cli` depends on `extract-lib`).
- Produces:
  - `static final int Main.EXIT_FATAL_ERROR = 70;`
  - `static Thread.UncaughtExceptionHandler Main.fatalErrorHandler(Runnable exitAction)` — returns a handler that, when the throwable is fatal per `ExtractionErrors.isFatal`, logs and runs `exitAction`; otherwise prints the stack trace and does not run `exitAction`. (`exitAction` is injected so the test can assert without killing the JVM; production passes `() -> System.exit(EXIT_FATAL_ERROR)`.)

- [ ] **Step 1: Write the failing test**

Create `extract-cli/src/test/java/org/icij/extract/cli/FatalErrorHandlerTest.java`:

```java
package org.icij.extract.cli;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.fest.assertions.Assertions.assertThat;

public class FatalErrorHandlerTest {

	@Test
	public void testFatalErrorTriggersExitAction() {
		final AtomicBoolean exited = new AtomicBoolean(false);
		final Thread.UncaughtExceptionHandler handler = Main.fatalErrorHandler(() -> exited.set(true));

		handler.uncaughtException(Thread.currentThread(), new OutOfMemoryError("synthetic"));

		assertThat(exited.get()).isTrue();
	}

	@Test
	public void testRecoverableErrorDoesNotTriggerExitAction() {
		final AtomicBoolean exited = new AtomicBoolean(false);
		final Thread.UncaughtExceptionHandler handler = Main.fatalErrorHandler(() -> exited.set(true));

		handler.uncaughtException(Thread.currentThread(), new StackOverflowError());

		assertThat(exited.get()).isFalse();
	}

	@Test
	public void testExitCodeIsSeventy() {
		assertThat(Main.EXIT_FATAL_ERROR).isEqualTo(70);
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-cli -am test -Dtest=FatalErrorHandlerTest`
Expected: FAIL — compilation error: `Main.fatalErrorHandler` and `Main.EXIT_FATAL_ERROR` do not exist.

- [ ] **Step 3: Add the constant, the handler factory, and install it**

In `Main.java`, add imports:

```java
import org.icij.extract.extractor.ExtractionErrors;
```

Add the field and factory method inside the class (e.g. just after the `taskFactory` field):

```java
	public static final int EXIT_FATAL_ERROR = 70;

	/**
	 * Build an uncaught-exception handler that runs {@code exitAction} when a thread dies from a fatal error
	 * (e.g. {@link OutOfMemoryError}), so the process can be restarted clean by an orchestrator. Recoverable
	 * throwables are logged but do not trigger the action.
	 *
	 * @param exitAction the action to run on a fatal error (production: {@code () -> System.exit(EXIT_FATAL_ERROR)})
	 * @return the handler
	 */
	static Thread.UncaughtExceptionHandler fatalErrorHandler(final Runnable exitAction) {
		return (thread, throwable) -> {
			if (ExtractionErrors.isFatal(throwable)) {
				System.err.println(String.format(
						"Fatal error on thread \"%s\"; exiting for a clean restart.", thread.getName()));
				throwable.printStackTrace();
				exitAction.run();
			} else {
				throwable.printStackTrace();
			}
		};
	}
```

At the very top of `main(String[] args)`, before registering tasks, install the handler:

```java
	public static void main(final String[] args) {
		Thread.setDefaultUncaughtExceptionHandler(fatalErrorHandler(() -> System.exit(EXIT_FATAL_ERROR)));

		taskFactory.addTask("clean-report", CleanReportTask.class);
		// ... existing registrations unchanged ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-cli -am test -Dtest=FatalErrorHandlerTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-cli/src/main/java/org/icij/extract/cli/Main.java \
        extract-cli/src/test/java/org/icij/extract/cli/FatalErrorHandlerTest.java
git commit -m "feat(cli): exit on fatal extraction errors for clean restart"
```

---

## Final verification

- [ ] **Build and test both modules**

Run: `mvn -pl extract-lib,extract-cli -am test -Dtest=ExtractionStatusTest,ExtractionErrorTest,ExtractionErrorsTest,ExtractorErrorBoundaryTest,DocumentConsumerErrorTest,FatalErrorHandlerTest`
Expected: PASS, all new tests green.

- [ ] **Full suite (requires Redis running, per README/Makefile)**

Run: `make test`
Expected: PASS, no regressions.

---

## Self-review notes (for the implementer)

- **Spec coverage:** Task 1 = `FAILURE_FATAL(10)`; Tasks 2-3 = wrapper + `isFatal`/`asException`; Task 4 = Extractor boundary + best-effort record + rethrow + extended `log`; Task 5 = `DocumentConsumer` boundary; Task 6 = CLI exit (code 70) reusing `isFatal`. The spec's "best-effort record must not mask" requirement is covered by Task 4 Step 1's `testRecordingFailureDoesNotMaskFatalError` and Step 3's recording try/catch.
- **Single source of truth:** Only `ExtractionErrors.isFatal` classifies; Extractor, DocumentConsumer, and Main all call it.
- **No `System.exit` in extract-lib:** the library only rethrows; exit lives in `Main` (Task 6), injected as `exitAction` so it is unit-testable.
- **Method-name consistency:** `isFatal`, `asException`, `fatalErrorHandler`, `EXIT_FATAL_ERROR`, `FAILURE_FATAL`, `sneakyThrow` are used identically across all referencing tasks.
