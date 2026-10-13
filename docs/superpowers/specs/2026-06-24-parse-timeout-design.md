# Wall-clock timeout on the general parse path

Date: 2026-06-24
Status: Approved (design)

## Problem

OCR has a per-image wall-clock watchdog (`Tess4JOCRParser.runWithTimeout`, `Tess4JOCRParser.java:653`), but the general Tika parse path has none. A parser stuck in an infinite loop pins a worker thread indefinitely. Enough such hangs and `DocumentConsumer`'s thread pool fills with zombies, throughput drops to zero, and no error is ever recorded.

The fix is to bound each document's parse with a `Future.get(timeout)` — the same pattern already trusted for OCR — so the blast radius of any hang is one slot plus one timeout, recorded as a failure.

## Relevant architecture

Parsing is **pull-driven and already runs off-thread**. `Extractor.getTikaDocument` builds a `ParsingReaderWithContentHandler`, which spawns a daemon thread running `parser.parse(...)` and writes results into a `PipedWriter`. The worker thread (a `DocumentConsumer` pool thread today) blocks reading the other end of that pipe via `spewer.write(document)`.

Consequences that shape the design:

- A hang manifests as the worker thread blocked in `reader.read()` on the pipe, while the daemon parse thread either spins or stalls.
- A thread in a true infinite loop **cannot be killed** by the JVM. Even after a timeout it lingers as a daemon zombie. The design accepts this (see Leak semantics).
- The parse executes lazily *during* `spewer.write()`. The parse cannot be timed in isolation from the output write — reading the pipe is what drives the parser. The watchdog therefore necessarily covers parse **and** spew together; a pathologically slow output sink (Solr/disk) would also trip it. This is forced by the architecture, not a design choice.
- `Spewer.write` already closes the root reader in a `finally`, so unwinding the worker (via interrupt) closes the reader and signals the daemon parse thread to stop on its next pipe write.

## Decisions

| Decision | Choice |
| --- | --- |
| Timeout policy | **Record + continue.** Record a failure, free the slot, keep processing. Accept that a CPU-spinning parser leaks its daemon thread until the next natural restart. |
| Placement | **Inside `Extractor`**, wrapping `extract(path)` + `spewer.write()` within `extract(Path, Spewer)`. Protects every extract-lib consumer, and the timeout flows through the existing error-recording path automatically. |
| Default | **Generous default, enabled.** 24h (`Duration.ofDays(1)`), comfortably above any realistic OCR-heavy parse. Operators tune via `--parseTimeout`; `0`/negative disables. |
| Mechanism | **A — Future + watchdog executor**, the OCR `runWithTimeout` pattern lifted to `Extractor`. |
| Status code | **New `ExtractionStatus.FAILURE_TIMEOUT(11)`**, additive, mirrors the recent `FAILURE_FATAL(10)`. |

## Design

### Configuration & components

New option on `Extractor` (mirrors `ocrTimeout`):

```java
@Option(name = "parseTimeout", description = "Wall-clock timeout for a single document's " +
        "parse and output, e.g. \"30m\" or \"24h\". Set to 0 to disable. Defaults to 24h.")
```

- Parsed with the existing duration parser:
  `options.get("parseTimeout", "24h").parse().asDuration().ifPresent(this::setParseTimeout)`.
- Field: `private Duration parseTimeout = Duration.ofDays(1);` (`0`/negative ⇒ disabled).
- Watchdog executor field:
  `private final ExecutorService parseExecutor = Executors.newCachedThreadPool(<daemon factory named "extract-watchdog-N">);`
  Cached so interrupted-and-recovered workers are reused; daemon so it never blocks JVM exit. Shared safely across pool threads — one in-flight worker per concurrent document, the same concurrency `DocumentConsumer` already imposes.

New status: `ExtractionStatus.FAILURE_TIMEOUT(11)`.

New exception: `ParseTimeoutException extends IOException`, carrying the path and duration for a clear message. Being an `IOException`, it fits the existing `extract(Path, Spewer) throws IOException` signature.

### Control flow & exception propagation

The current body of `extract(Path, Spewer)` (the `extract(path)` + `spewer.write(document)` pair) moves into a private `doExtract(Path, Spewer)`, which also closes the document reader in a `finally` (covers the rare hang in the pre-spew `extract(path)` metadata read — TIKA-203's first-character buffering).

```java
public void extract(Path path, Spewer spewer) throws IOException {
    if (parseTimeout == null || parseTimeout.isZero() || parseTimeout.isNegative()) {
        doExtract(path, spewer);            // exact current behavior, no watchdog thread
        return;
    }
    Future<?> future = parseExecutor.submit(() -> { doExtract(path, spewer); return null; });
    try {
        future.get(parseTimeout.toMillis(), MILLISECONDS);
    } catch (TimeoutException e) {
        future.cancel(true);                // interrupt worker -> unblocks pipe read
                                            // -> Spewer.write finally closes reader -> daemon stops
        throw new ParseTimeoutException(path, parseTimeout);
    } catch (ExecutionException e) {
        sneakyThrow(e.getCause());          // propagate worker's original Throwable verbatim
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.cancel(true);
        throw new IOException("extraction interrupted", e);
    }
}
```

Correctness invariant: **`ExecutionException` is unwrapped and the worker's original `Throwable` is rethrown verbatim** (`sneakyThrow`, already present in the file). A parser's `IOException`, `RuntimeException`, or fatal `Error` (OutOfMemoryError / StackOverflowError) therefore reaches `extract(Path, Spewer, Reporter)` exactly as it does today. The existing recoverable-vs-fatal classification and the fatal → process-exit path are **unchanged**. Only a genuine wall-clock timeout becomes the new `ParseTimeoutException`.

### Status recording

`ParseTimeoutException` is an `IOException`, so it lands in the existing `catch (Throwable t)` of `extract(Path, Spewer, Reporter)` as an ordinary `Exception` (not an `Error`) — no fatal propagation, record + continue.

Two small additions route it to the new status:

- In `status(Exception, Spewer)`: an early branch —
  `if (e instanceof ParseTimeoutException) return ExtractionStatus.FAILURE_TIMEOUT;`
  placed before the generic `IOException` handling.
- In `log(...)`: a `case FAILURE_TIMEOUT:` → `logger.error("Parse exceeded timeout: \"%s\".", file)`.

`reporter.save(path, FAILURE_TIMEOUT, exception)` then records through the unchanged path, and the pool thread returns — slot freed.

### Leak semantics

Explicit, per the record + continue choice:

- **Common case** (parser blocked on I/O, or looping but periodically writing to the pipe): `cancel(true)` interrupts the worker out of the pipe read; `Spewer.write`'s `finally` closes the reader; the daemon parse thread dies on its next pipe write. Nothing leaks — the worker returns to the cached pool.
- **Pathological case** (parser in a tight CPU loop that never touches the pipe and ignores interrupts): the daemon parse thread cannot be killed and leaks until the next natural process restart. Accepted cost. The watchdog worker itself still recovers (it was only blocked on the pipe read). Worst case is one leaked daemon thread per pathological document — bounded by how many such documents exist, not by throughput.

## Testing

Deterministic unit tests in `extract-lib`:

1. **Timeout recorded as `FAILURE_TIMEOUT`.**
   - Stub `Parser` whose `parse(...)` blocks on an unreleased `CountDownLatch` (deterministic hang — never completes regardless of timing).
   - `Extractor` configured with that parser and a small `parseTimeout` (e.g. `200ms`).
   - Call `extract(path, spewer, reporter)` with a recording stub reporter.
   - Assert: the call returns (no fatal throw) and `reporter.save` was invoked with `FAILURE_TIMEOUT`.
   - Teardown: count the latch down so the stub parse thread exits — no leaked threads across the suite.
   - Determinism: the hang is latch-based, not timing-based; the only timing assumption is "200ms < test timeout," which is robust. Mirrors the determinism care in the recent `DocumentConsumer` test work.

2. **Disable path.** `parseTimeout = 0` runs `doExtract` directly (no watchdog) and a normal document still extracts successfully.

## Out of scope

- Killing or interrupting CPU-bound parsers that ignore interrupts (impossible on the JVM without process-level isolation).
- Exit-for-restart on timeout, and exit-after-N-leaks thresholds — considered and rejected in favor of record + continue.
- Per-phase (parse-only vs spew-only) timeouts — impossible under the pull-driven pipe model.
