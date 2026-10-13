# Throwable boundary for the extraction worker

**Date:** 2026-06-24
**Status:** Approved — ready for implementation planning
**Scope:** `extract-lib` (`Extractor`, `DocumentConsumer`, `ExtractionStatus`) and `extract-cli` (`Main`)

## Problem

`Extractor.extract(Path, Spewer, Reporter)` (`Extractor.java:388`) and `DocumentConsumer.accept` both catch only `Exception`. When a Tika parser throws an `Error` — `StackOverflowError` (deeply nested XML/zip/PST), `OutOfMemoryError` (zip bomb, huge embedded image), `LinkageError`, etc. — it escapes both catch blocks. Consequences:

1. The document never reaches `reporter.save(...)`, so it gets **no terminal status**. Depending on queue semantics it is either silently lost or re-queued forever — a poison pill that stalls every worker that picks it up.
2. The pool worker thread dies via the default uncaught-exception handler.
3. An `OutOfMemoryError` can leave the whole JVM in a degraded state, causing spurious downstream failures.

For a pipeline whose entire purpose is digesting millions of heterogeneous and often adversarial documents (leaked dumps), `StackOverflowError` and `OutOfMemoryError` are *expected* inputs, not exceptional ones.

## Goals

- Every document always receives a recorded terminal `ExtractionStatus`, including when a parser throws an `Error`.
- A recoverable `Error` (e.g. `StackOverflowError`) on one file does not kill the worker — processing continues with the next file.
- An `OutOfMemoryError` is recorded best-effort, then the process exits so an orchestrator (Docker/systemd/k8s) restarts it clean, rather than continuing on a degraded heap.
- `extract-lib` remains free of `System.exit`; process lifecycle stays owned by the application layer (`extract-cli`).

## Non-goals (YAGNI)

- Per-document parse timeouts / watchdog for the general Tika path (tracked separately as robustness item #2).
- Zip-bomb / write-limit / embedded-depth guards (`SecureContentHandler`, etc. — robustness item #3).
- Retry/backoff policy for `FAILURE_FATAL` documents. The status is recorded distinctly so operators *can* re-queue them, but automated retry is out of scope.

## Core distinction: recoverable vs. fatal

`StackOverflowError` is itself a subclass of `VirtualMachineError`, so errors cannot be classified by `VirtualMachineError` membership alone. A single shared predicate is the source of truth for both the rethrow decision (library) and the process-exit decision (CLI):

```java
static boolean isFatalError(Throwable t) {
    return (t instanceof OutOfMemoryError)
        || (t instanceof VirtualMachineError && !(t instanceof StackOverflowError));
    // Fatal:       OutOfMemoryError, InternalError, UnknownError
    // Recoverable: StackOverflowError, LinkageError, AssertionError, and all Exceptions
}
```

This predicate lives in `extract-lib` (a static helper on `Extractor`, or a small dedicated util class) and is reused by the CLI uncaught handler so the two definitions can never drift apart.

## Components

### 1. `ExtractionStatus.FAILURE_FATAL(10)`

New enum constant. Code `10` (next free; `0,1,2,3,4,8,9` are taken). Semantics: "a parser threw an `Error`." Kept distinct from `FAILURE_NOT_PARSED(4)` so these documents are separately queryable and re-queueable in reports and queues. Any exhaustive `switch` on `ExtractionStatus` (e.g. `Extractor.log`) must handle the new value; the `default` branch already covers it for logging.

### 2. Error → Exception wrapper

`Reporter.save(Path, ExtractionStatus, Exception)`, `Report`, the journal (`journalableTypes : Set<Class<? extends Exception>>`), and the Redis serialization are all typed on `Exception`. To avoid rippling `Throwable` through the serialization layer, a caught `Error` is wrapped before recording:

- A small `ExtractionError extends Exception` that preserves the original `Throwable` as its cause.
- `Exception`s pass through unchanged (existing behavior).

Helper: `asException(Throwable t)` → returns `t` if it is already an `Exception`, otherwise `new ExtractionError(t)`.

### 3. `Extractor.extract(Path, Spewer, Reporter)`

Change `catch (final Exception e)` to `catch (final Throwable t)`. Restructure so recording happens before any rethrow:

```
try {
    extract(path, spewer);                // SUCCESS path unchanged
} catch (final Throwable t) {
    status    = (t instanceof Exception) ? status((Exception) t, spewer)
                                         : ExtractionStatus.FAILURE_FATAL;
    exception = asException(t);
    log(...);                              // existing logging, extended for FAILURE_FATAL

    // best-effort record — must not mask the original error
    try {
        reporter.save(path, status, unwrapTagged(exception));
    } catch (Throwable recordingFailure) {
        logger.error("Failed to record status for \"" + path + "\"", recordingFailure);
    }

    if (isFatalError(t)) {
        throw t;                           // propagate OOM etc. for process restart
    }
    return;                                // recoverable Error / Exception already recorded
}
reporter.save(path, status, exception);    // SUCCESS path (unchanged)
```

- The existing tagged-IOException unwrapping (`Extractor.java:395`) is applied on the failure path before recording.
- `Exception`s retain their current status mapping via the existing `status(Exception, Spewer)` method — no behavior change for normal failures.
- Recoverable `Error`s (`StackOverflowError`, `LinkageError`) are recorded as `FAILURE_FATAL` and the method returns normally.

### 4. `DocumentConsumer.accept`

The submitted `Runnable` becomes the uniform worker boundary. Change `catch (Exception e)` to `catch (Throwable t)`:

- Log the throwable.
- If `isFatalError(t)` → rethrow so it reaches the pool thread's uncaught-exception handler.
- Otherwise swallow (already logged).

Rationale: when a reporter is set, `Extractor.extract(reporter)` already records and only rethrows fatal errors, so `DocumentConsumer` just decides thread survival. When no reporter is set, `DocumentConsumer` calls `extract(Path, Spewer)` (no recording); catching `Throwable` here ensures a recoverable `Error` in that mode still does not kill the worker. No double-recording occurs — `Extractor` owns recording, `DocumentConsumer` owns thread survival.

### 5. CLI uncaught-exception handler (`extract-cli`)

`Main` installs `Thread.setDefaultUncaughtExceptionHandler` at bootstrap. On a throwable for which `isFatalError(t)` is true: log a clear fatal message and `System.exit(EXIT_FATAL_ERROR)`. `EXIT_FATAL_ERROR` is a dedicated documented non-zero code (`70`), distinct from the existing `1`/`2` usage in `Main`. `extract-lib` itself never calls `System.exit`.

## Data flow

| Scenario | Recorded status | Worker thread | Process |
|---|---|---|---|
| Normal parse failure (`TikaException`, `IOException`, …) | unchanged (`FAILURE_NOT_PARSED`, etc.) | survives | continues |
| `StackOverflowError` in a parser | `FAILURE_FATAL` | survives | continues |
| `LinkageError` / `AssertionError` | `FAILURE_FATAL` | survives | continues |
| `OutOfMemoryError` / `InternalError` | `FAILURE_FATAL` (best-effort) | dies | `System.exit(70)` → orchestrator restart |

## Testing (TDD — write tests first, confirm they fail, then implement)

- **`isFatalError`** unit tests: `OutOfMemoryError`→true, `StackOverflowError`→false, `InternalError`→true, `RuntimeException`→false, `IOException`→false.
- **`Extractor` + recoverable Error**: register a stub parser via `addParser` (commit 63abc95) that throws `new StackOverflowError()` → assert reporter records `FAILURE_FATAL` and `extract` returns without throwing.
- **`Extractor` + fatal Error**: stub parser throws `new OutOfMemoryError()` (synthetic instance — do not actually exhaust the heap) → assert reporter records `FAILURE_FATAL` **and** the error is rethrown.
- **Best-effort record**: a reporter whose `save` throws on the fatal path → assert the original `OutOfMemoryError` still propagates (not masked by the recording failure).
- **`DocumentConsumer`**: fatal error propagates past `accept`; recoverable error is swallowed and logged (worker survives).
- **`ExtractionError` wrapper**: preserves the original throwable as cause; round-trips through `Report` serialization.

## Affected files

- `extract-lib/.../extractor/ExtractionStatus.java` — add `FAILURE_FATAL(10)`.
- `extract-lib/.../extractor/Extractor.java` — `Throwable` boundary, `isFatalError`, `asException`, extended `log`.
- `extract-lib/.../extractor/ExtractionError.java` — new wrapper class.
- `extract-lib/.../extractor/DocumentConsumer.java` — `Throwable` boundary in the worker task.
- `extract-cli/.../cli/Main.java` — default uncaught-exception handler + `EXIT_FATAL_ERROR`.
- Corresponding tests under each module's `src/test`.
