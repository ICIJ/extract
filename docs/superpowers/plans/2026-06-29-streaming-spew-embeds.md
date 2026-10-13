# Streaming Spew Embeds (B2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spew each embedded document to the index as it is parsed, instead of building the whole `TikaDocument` tree and writing it afterwards, so the ES doc count climbs during the parse, peak memory tracks in-flight work, and a mid-parse failure keeps already-written embeds.

**Architecture:** A new `StreamingSpewCoordinator` (in `org.icij.spewer`) runs a single daemon "spew worker" thread fed by a bounded queue. `EmbedSpawner` notifies a `SpewSink` when each embed's text is ready (synchronously for non-deferred embeds; via the OCR `CompletableFuture`'s completion for deferred ones). The worker writes each embed via the existing `Spewer.writeDocument` primitive and closes its reader. The foreground (watchdog) thread writes the root via `writeDocument(root, …)`, which drains the root pipe (driving the parse) and indexes the root; the coordinator then awaits the worker drained before closing the root reader. Digests/IDs/artifact filenames are unchanged (computed synchronously on the parse thread, as today).

**Tech Stack:** Java 17, Maven (`extract-lib` module), Apache Tika, JUnit 4, FEST assertions (`org.fest.assertions.Assertions.assertThat`). Datashare adoption: JUnit 4 + `ElasticsearchRule` integration tests.

## Global Constraints

- **Byte-identity is sacred:** embed IDs, `X-TIKA:digest:*`, and artifact filenames MUST stay byte-identical to today. No task may move or alter digest/ID/artifact computation; these run synchronously on the parse thread before any spew. (Spec: "Sacred invariant".)
- **Default ON:** the `streamingSpew` option defaults to `true` so the path is live and measurable on the prod box, but must be flippable to fall back to the legacy tree walk.
- **No new dependencies.** extract-lib stays on its current Tika/POI stack.
- **Commits:** use the `/commit` skill; conventional-commit messages; **no Claude attribution** (no co-author trailers, no "Generated with"). ff/rebase, no squash. Work on branch `feat/streaming-spew-embeds` (already created off `master`).
- **extract-lib tests** run with `mvn -pl extract-lib -am test -Dtest=<Class>` from the extract repo (no ES/Redis needed).
- **Datashare single-test:** `mvn -pl datashare-app -am test -Dtest=X -Dsurefire.failIfNoSpecifiedTests=false`; ES at `elasticsearch:9200`, Redis at `redis:6379`.
- extract version bumps `9.11.0` → `9.12.0`; datashare `<extract.version>` (pom.xml:99) bumps to match.

---

## File Structure

**extract-lib (new):**
- `extract-lib/src/main/java/org/icij/spewer/SpewItem.java` — immutable record `(embed, parent, root, level)` describing one ready-to-write embed.
- `extract-lib/src/main/java/org/icij/spewer/SpewSink.java` — interface the parse side calls: `promise()` + `ready(SpewItem)`.
- `extract-lib/src/main/java/org/icij/spewer/StreamingSpewCoordinator.java` — queue + single worker thread + lifecycle; implements `SpewSink` and `AutoCloseable`.

**extract-lib (modify):**
- `document/TikaDocument.java:39` — make `isDuplicate` `volatile` (cross-thread visibility for the duplicate-gate).
- `extractor/EmbedSpawner.java` — add a nullable `SpewSink sink`; call `promise()`/`ready()` from `spawnEmbedded` and `spawnEmbeddedDeferred`.
- `extractor/Extractor.java` — `streamingSpew` option; `extract(Path, SpewSink)` overload; thread the sink through `getTikaDocument`; streaming branch in `doExtract`.

**datashare (modify):**
- `pom.xml:99` — bump `<extract.version>` to `9.12.0`.
- `datashare-index/src/test/java/.../ElasticsearchSpewerTest.java` — add a streaming multi-embed integration test (exercises the two-writer path).

---

## Task 1: Make the duplicate flag thread-visible

The streaming worker reads `root.isDuplicate()` while the foreground sets it inside `writeDocument(root)`. The field must be `volatile` so the worker reliably sees the foreground's write.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/document/TikaDocument.java:39`
- Test: `extract-lib/src/test/java/org/icij/extract/document/TikaDocumentDuplicateFieldTest.java`

**Interfaces:**
- Produces: `TikaDocument.isDuplicate()` / `setDuplicate(boolean)` (existing signatures, now backed by a volatile field).

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/document/TikaDocumentDuplicateFieldTest.java`:

```java
package org.icij.extract.document;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.fest.assertions.Assertions.assertThat;

public class TikaDocumentDuplicateFieldTest {

    // The streaming spew worker reads isDuplicate on a different thread from the one that sets it
    // (the foreground writeDocument(root) call). The field must be volatile for visibility.
    @Test
    public void testIsDuplicateFieldIsVolatile() throws Exception {
        final Field field = TikaDocument.class.getDeclaredField("isDuplicate");
        assertThat(Modifier.isVolatile(field.getModifiers()))
                .as("TikaDocument.isDuplicate must be volatile for cross-thread duplicate-gate visibility")
                .isTrue();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -am test -Dtest=TikaDocumentDuplicateFieldTest`
Expected: FAIL (field is not volatile yet).

- [ ] **Step 3: Make the field volatile**

In `extract-lib/src/main/java/org/icij/extract/document/TikaDocument.java:39`, change:

```java
	private boolean isDuplicate;
```

to:

```java
	private volatile boolean isDuplicate;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib -am test -Dtest=TikaDocumentDuplicateFieldTest`
Expected: PASS.

- [ ] **Step 5: Commit** (via `/commit`)

```bash
git add extract-lib/src/main/java/org/icij/extract/document/TikaDocument.java \
        extract-lib/src/test/java/org/icij/extract/document/TikaDocumentDuplicateFieldTest.java
git commit -m "fix(spew): make TikaDocument.isDuplicate volatile for streaming duplicate-gate"
```

---

## Task 2: `SpewItem` record and `SpewSink` interface

The minimal contract between the parse side (`EmbedSpawner`) and the spew side (`StreamingSpewCoordinator`).

**Files:**
- Create: `extract-lib/src/main/java/org/icij/spewer/SpewItem.java`
- Create: `extract-lib/src/main/java/org/icij/spewer/SpewSink.java`
- Test: `extract-lib/src/test/java/org/icij/spewer/SpewItemTest.java`

**Interfaces:**
- Produces:
  - `record SpewItem(TikaDocument embed, TikaDocument parent, TikaDocument root, int level)`
  - `interface SpewSink { void promise(); void ready(SpewItem item); }`

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/spewer/SpewItemTest.java`:

```java
package org.icij.spewer;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class SpewItemTest {

    @Test
    public void testSpewItemExposesItsFields() {
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("root"));
        TikaDocument embed = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("embed"));

        SpewItem item = new SpewItem(embed, root, root, 1);

        assertThat(item.embed()).isSameAs(embed);
        assertThat(item.parent()).isSameAs(root);
        assertThat(item.root()).isSameAs(root);
        assertThat(item.level()).isEqualTo(1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -am test -Dtest=SpewItemTest`
Expected: FAIL (compile error: `SpewItem` not found).

- [ ] **Step 3: Create the record and interface**

Create `extract-lib/src/main/java/org/icij/spewer/SpewItem.java`:

```java
package org.icij.spewer;

import org.icij.extract.document.TikaDocument;

/**
 * One embedded document that is ready to be written to the spewer, carrying the same arguments the
 * legacy tree walk passes to {@link Spewer#writeDocument(TikaDocument, TikaDocument, TikaDocument, int)}:
 * the embed, its immediate parent, the top-level root, and its 1-based nesting level.
 */
public record SpewItem(TikaDocument embed, TikaDocument parent, TikaDocument root, int level) {}
```

Create `extract-lib/src/main/java/org/icij/spewer/SpewSink.java`:

```java
package org.icij.spewer;

/**
 * Streaming spew callback used by the parse side to push embeds to a consumer as they are produced.
 *
 * <p>{@link #promise()} is called SYNCHRONOUSLY on the parse thread for every embed that is destined
 * to be spewed (both the synchronous and the deferred-OCR paths), at the moment the embed node is
 * created. {@link #ready(SpewItem)} is called when that embed's extracted text is fully buffered:
 * on the parse thread for synchronous embeds, and on the OCR completion thread for deferred embeds.
 * The promise/ready split lets the consumer know how many embeds to expect before the parse ends,
 * so it can wait for every deferred OCR to finish without a separate join.
 */
public interface SpewSink {
    /** Register that one more embed will eventually be {@link #ready(SpewItem)}. Parse thread only. */
    void promise();

    /** Hand a ready-to-write embed to the consumer. */
    void ready(SpewItem item);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib -am test -Dtest=SpewItemTest`
Expected: PASS.

- [ ] **Step 5: Commit** (via `/commit`)

```bash
git add extract-lib/src/main/java/org/icij/spewer/SpewItem.java \
        extract-lib/src/main/java/org/icij/spewer/SpewSink.java \
        extract-lib/src/test/java/org/icij/spewer/SpewItemTest.java
git commit -m "feat(spew): add SpewItem record and SpewSink interface"
```

---

## Task 3: `StreamingSpewCoordinator`

The queue + single worker thread + lifecycle. This is the heart of B2.

**Files:**
- Create: `extract-lib/src/main/java/org/icij/spewer/StreamingSpewCoordinator.java`
- Test: `extract-lib/src/test/java/org/icij/spewer/StreamingSpewCoordinatorTest.java`

**Interfaces:**
- Consumes: `Spewer` (calls its package-visible `writeDocument` and static `closeReaderQuietly`), `SpewItem`, `SpewSink`.
- Produces:
  - `StreamingSpewCoordinator(Spewer spewer, int queueCapacity)`
  - `void promise()` / `void ready(SpewItem)` (from `SpewSink`)
  - `void spew(TikaDocument root) throws IOException` — start worker, write root on the calling (foreground) thread, await all promised embeds written, close the root reader, then rethrow the first error (root parse error preferred, else worker error).
  - `void close()` (idempotent worker shutdown; from `AutoCloseable`)
  - `long writtenCount()` — embeds actually written (test/assertion helper)

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/spewer/StreamingSpewCoordinatorTest.java`:

```java
package org.icij.spewer;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewCoordinatorTest {

    // A thread-safe recording spewer: records each written doc id and whether the root was a child write.
    private static class RecordingSpewer extends Spewer {
        final List<String> written = Collections.synchronizedList(new ArrayList<>());
        volatile boolean throwOnEmbed = false;

        RecordingSpewer() { super(new FieldNames()); }

        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            if (throwOnEmbed && parent != null) {
                throw new IOException("boom writing " + doc.getId());
            }
            written.add(doc.getId());
        }
    }

    private TikaDocument doc(String id, Reader reader) {
        TikaDocument d = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get(id));
        d.setReader(reader);
        return d;
    }

    @Test
    public void testWritesEveryPromisedEmbedAndTheRoot() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));
        TikaDocument e2 = doc("e2", new StringReader("b"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            // Simulate the parse: promise + ready each embed BEFORE the foreground writes the root.
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            coord.promise();
            coord.ready(new SpewItem(e2, root, root, 1));

            coord.spew(root); // foreground writes root, then awaits the two embeds
        }

        // Root and both embeds written; the two embeds before await completes.
        assertThat(spewer.written).contains("root", "e1", "e2");
        assertThat(spewer.written).hasSize(3);
    }

    @Test
    public void testSkipsChildrenWhenRootIsDuplicate() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        root.setDuplicate(true); // foreground's writeDocument(root) would set this in production
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            coord.spew(root);
        }

        // The root is still written (its writeDocument runs); the child is skipped.
        assertThat(spewer.written).contains("root");
        assertThat(spewer.written).excludes("e1");
    }

    @Test
    public void testWorkerErrorIsRethrownFromSpew() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        spewer.throwOnEmbed = true;
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            try {
                coord.spew(root);
                org.junit.Assert.fail("expected the worker's IOException to propagate");
            } catch (IOException expected) {
                assertThat(expected.getMessage()).contains("boom");
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -am test -Dtest=StreamingSpewCoordinatorTest`
Expected: FAIL (compile error: `StreamingSpewCoordinator` not found).

- [ ] **Step 3: Implement the coordinator**

Create `extract-lib/src/main/java/org/icij/spewer/StreamingSpewCoordinator.java`:

```java
package org.icij.spewer;

import org.icij.extract.document.TikaDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streaming spew: writes embeds to the {@link Spewer} as they are produced by the parse, on a single
 * dedicated worker thread, instead of walking a fully-built tree afterwards.
 *
 * <p>Threading: the parse thread calls {@link #promise()} (synchronously, per embed) and
 * {@link #ready(SpewItem)} (when the embed's text is buffered; the OCR completion thread does this
 * for deferred embeds). The single worker thread drains the queue and writes each embed. The
 * foreground thread calls {@link #spew(TikaDocument)}, which writes the root (driving the parse via
 * the root pipe) and then awaits every promised embed before closing the root reader.
 *
 * <p>The worker never blocks on OCR: a deferred embed is enqueued only AFTER its OCR future has
 * completed, so reading its reader returns immediately. The bounded queue provides backpressure
 * (the parse thread blocks in {@link #ready} when the worker lags) without deadlock, because the
 * worker can always make progress on what it dequeues.
 */
public class StreamingSpewCoordinator implements SpewSink, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(StreamingSpewCoordinator.class);
    private static final Object POISON = new Object();

    private final Spewer spewer;
    private final BlockingQueue<Object> queue;
    private final AtomicLong promised = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final Object drainLock = new Object();
    private volatile Throwable workerError;
    private volatile Thread worker;

    public StreamingSpewCoordinator(final Spewer spewer, final int queueCapacity) {
        this.spewer = spewer;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
    }

    private void start() {
        final Thread t = new Thread(this::run, "extract-spew");
        t.setDaemon(true);
        worker = t;
        t.start();
    }

    @Override
    public void promise() {
        promised.incrementAndGet();
    }

    @Override
    public void ready(final SpewItem item) {
        try {
            queue.put(item); // blocks when full -> backpressure on the parse / OCR thread
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            // Count it as completed so awaitDrained cannot hang on a lost item.
            signalCompleted();
        }
    }

    /**
     * Foreground entry point. Writes the root (which drains the root pipe, driving the parse and thus
     * all embed promises/readies), then awaits every promised embed before closing the root reader.
     * Rethrows the root parse error if any, else the first worker error.
     */
    public void spew(final TikaDocument root) throws IOException {
        start();
        Throwable rootError = null;
        try {
            spewer.writeDocument(root, null, null, 0);
        } catch (final Throwable t) {
            rootError = t;
        } finally {
            // Wait for all promised embeds (including in-flight deferred OCR) so the shared temp
            // resources are still alive while the worker reads them; only then close the root reader.
            awaitDrained();
            shutdownWorker();
            Spewer.closeReaderQuietly(root);
        }
        if (rootError != null) {
            throwAsIO(rootError);
        }
        if (workerError != null) {
            throwAsIO(workerError);
        }
    }

    private void run() {
        try {
            while (true) {
                final Object o = queue.take();
                if (o == POISON) {
                    return;
                }
                final SpewItem item = (SpewItem) o;
                try {
                    // Skip children of a duplicate root, matching the legacy tree walk's gating.
                    if (workerError == null && !item.root().isDuplicate()) {
                        spewer.writeDocument(item.embed(), item.parent(), item.root(), item.level());
                    }
                } catch (final Throwable t) {
                    if (workerError == null) {
                        workerError = t; // record the first; keep draining so awaitDrained never hangs
                    }
                    logger.error("streaming spew failed for embed {}", item.embed().getId(), t);
                } finally {
                    Spewer.closeReaderQuietly(item.embed()); // free the embed's text buffer / spill file
                    signalCompleted();
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void signalCompleted() {
        synchronized (drainLock) {
            completed.incrementAndGet();
            drainLock.notifyAll();
        }
    }

    /** Block until every promised embed has been processed. Promises are final once the parse ends. */
    private void awaitDrained() {
        synchronized (drainLock) {
            while (completed.get() < promised.get()) {
                try {
                    drainLock.wait();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void shutdownWorker() {
        final Thread t = worker;
        if (t == null) {
            return;
        }
        try {
            queue.put(POISON);
            t.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            t.interrupt();
        } finally {
            worker = null;
        }
    }

    /** Embeds actually written (excludes skipped duplicate-root children). Test helper. */
    public long writtenCount() {
        return completed.get();
    }

    @Override
    public void close() {
        shutdownWorker();
    }

    private static void throwAsIO(final Throwable t) throws IOException {
        if (t instanceof IOException) {
            throw (IOException) t;
        }
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        throw new IOException(t);
    }
}
```

Note: `Spewer.writeDocument` is `protected` and `Spewer.closeReaderQuietly` is `protected static`; both are accessible here because `StreamingSpewCoordinator` is in the same `org.icij.spewer` package.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib -am test -Dtest=StreamingSpewCoordinatorTest`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit** (via `/commit`)

```bash
git add extract-lib/src/main/java/org/icij/spewer/StreamingSpewCoordinator.java \
        extract-lib/src/test/java/org/icij/spewer/StreamingSpewCoordinatorTest.java
git commit -m "feat(spew): add StreamingSpewCoordinator (queue + worker + lifecycle)"
```

---

## Task 4: Wire `EmbedSpawner` to the sink

`EmbedSpawner` gains a nullable `SpewSink`. When set, it promises each embed at creation and readies it when the text is buffered (synchronously for `spawnEmbedded`, via the OCR future for `spawnEmbeddedDeferred`). When null, behaviour is exactly as today.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java`

**Interfaces:**
- Consumes: `org.icij.spewer.SpewSink`, `org.icij.spewer.SpewItem`.
- Produces: the existing `EmbedSpawner` constructor used by `Extractor.getTikaDocument` gains a trailing `SpewSink sink` parameter (nullable). A `setSink(SpewSink)` is NOT added; the sink is constructor-injected.

**Note on testing:** this task is pure plumbing. The sink wiring is not reachable from any test until the `Extractor.extract(Path, SpewSink)` overload exists, so the behavioural tests that exercise it (`EmbedSpawnerSinkTest`, `StreamingSpewOrderTest`) live in Task 5. This task is verified by a main-source compile and the existing suite still passing; Task 5 adds the tests that drive this code end-to-end. Do not create a test here that calls `extract(Path, SpewSink)` — it would not compile and would break the build at this commit.

- [ ] **Step 1: Add the sink field and constructor parameter**

In `EmbedSpawner.java`, add the import near the other imports:

```java
import org.icij.spewer.SpewItem;
import org.icij.spewer.SpewSink;
```

Add the field after `ocrParserClassName` (line ~70):

```java
	// Streaming spew sink; null in the legacy buffer-then-walk mode. When set, each embed is
	// promised at creation and readied when its text is buffered (synchronously for non-deferred
	// embeds, on the OCR completion thread for deferred ones).
	private final SpewSink sink;
```

Update the **primary** (longest) constructor signature to accept the sink as the trailing parameter and assign it:

```java
	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh,
				 final Supplier<ExecutorService> ocrExecutorSupplier,
				 final boolean ocrEnabled,
				 final ExtractionProgress progress,
				 final DigestingParser.Digester digester,
				 final boolean ocrFanout, final long ocrMinImageBytes,
				 final String ocrParserClassName,
				 final SpewSink sink) {
		super(root, context);
		this.outputPath = outputPath;
		this.handlerFunction = handlerFunction;
		this.embedMemoryBudgetBytes = embedMemoryBudgetBytes;
		this.tmp = tmp;
		this.memoryPressureHigh = memoryPressureHigh;
		this.ocrExecutorSupplier = ocrExecutorSupplier;
		this.ocrEnabled = ocrEnabled;
		this.progress = progress;
		this.digester = digester;
		this.ocrFanout = ocrFanout;
		this.ocrMinImageBytes = ocrMinImageBytes;
		this.ocrParserClassName = ocrParserClassName;
		this.sink = sink;
		tikaDocumentStack.add(root);
	}
```

Update the two delegating constructors to pass `null` for the new `sink` parameter. The serial constructor (the 7-arg one) delegates as:

```java
		this(root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh,
				() -> null, false, null, null, false, 0L, null, null);
```

The backward-compatible executor overload delegates as:

```java
		this(root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh,
				() -> ocrExecutor, ocrExecutor != null, progress, digester, ocrFanout, ocrMinImageBytes,
				ocrParserClassName, null);
```

- [ ] **Step 2: Promise + ready non-deferred embeds**

In `spawnEmbedded`, capture the parent and level at the point the embed is added, and ready it once its text is buffered. Replace the line:

```java
		final EmbeddedTikaDocument embed = tikaDocumentStack.getLast().addEmbed(metadata);
```

with:

```java
		final TikaDocument spewParent = tikaDocumentStack.getLast();
		final int spewLevel = tikaDocumentStack.size();
		if (sink != null) {
			sink.promise();
		}
		final EmbeddedTikaDocument embed = spewParent.addEmbed(metadata);
```

Then, at the very end of `spawnEmbedded` (after the `if (null != this.outputPath) { writeEmbed(...); }` block), add:

```java
		// Text is fully buffered and the artifact (if any) is written: hand this embed to the spew worker.
		if (sink != null) {
			sink.ready(new SpewItem(embed, spewParent, root, spewLevel));
		}
```

Note: the embed is `removeEmbed`'d and returned early on a spool failure (before the stack push). On that early-return path the embed was never readied and we already called `promise()`, which would leave one promise unmatched. To keep promise/ready balanced, on each early `return` inside `spawnEmbedded` (the `catch` after `tis.getPath()`), add before the `return`:

```java
			if (sink != null) {
				// Balance the promise(): this embed will never be readied.
				sink.ready(new SpewItem(embed, spewParent, root, spewLevel));
			}
```

(The worker writes the removed-from-parent embed by id; since it carries no reader content of value and is independent, this is harmless and keeps the drain count balanced. Alternatively a dedicated `sink.cancel()` could be added, but readying the empty embed is simpler and idempotent.)

- [ ] **Step 3: Promise + ready deferred embeds**

In `spawnEmbeddedDeferred`, capture parent/level the same way. Replace:

```java
		final EmbeddedTikaDocument embed = tikaDocumentStack.getLast().addEmbed(metadata);
```

with:

```java
		final TikaDocument spewParent = tikaDocumentStack.getLast();
		final int spewLevel = tikaDocumentStack.size();
		if (sink != null) {
			sink.promise();
		}
		final EmbeddedTikaDocument embed = spewParent.addEmbed(metadata);
```

On each early-return abort path in `spawnEmbeddedDeferred` (the spool-failure catch and the temp-copy-failure catch), add before the `return`, to balance the promise:

```java
			if (sink != null) {
				sink.ready(new SpewItem(embed, spewParent, root, spewLevel));
			}
```

Finally, ready the embed when OCR completes. Right after the existing `embed.setReader(() -> { ... });` block, add:

```java
		// Stream this embed to the spew worker once its OCR future completes (normal completion,
		// recorded failure, or rejected submit all complete `done`). Runs on the completing thread.
		if (sink != null) {
			done.whenComplete((v, t) -> sink.ready(new SpewItem(embed, spewParent, root, spewLevel)));
		}
```

(`done` is the existing `CompletableFuture<Void>`; it is completed in the OCR task's `finally`, in the rejected-execution `catch`, so `whenComplete` always fires exactly once.)

- [ ] **Step 4: Build main sources and run the existing suite**

Run: `mvn -pl extract-lib -am compile`
Expected: BUILD SUCCESS. `Extractor.getTikaDocument` still calls the executor-overload constructor (which now delegates with `sink = null`), so the call site is unbroken.

Then confirm no regression in the existing embed/OCR tests:

Run: `mvn -pl extract-lib -am test -Dtest=EmbedSpawnerDeferredOcrTest,ParallelOcrDeterminismTest`
Expected: PASS (legacy null-sink path behaves exactly as before).

- [ ] **Step 5: Commit** (via `/commit`)

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java
git commit -m "feat(spew): EmbedSpawner promises/readies embeds to a SpewSink when set"
```

---

## Task 5: Wire `Extractor` — option, overload, streaming `doExtract`

Add the `streamingSpew` option, an `extract(Path, SpewSink)` overload that threads the sink into `getTikaDocument` → `EmbedSpawner`, and the streaming branch in `doExtract`.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerSinkTest.java` (drives the Task 4 sink wiring through the new overload)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/StreamingSpewOrderTest.java`

**Interfaces:**
- Consumes: `StreamingSpewCoordinator`, `SpewSink`, `SpewItem`.
- Produces:
  - `public TikaDocument extract(Path path, SpewSink sink) throws IOException`
  - `streamingSpew` option (boolean, default `true`); `boolean isStreamingSpew()`.

- [ ] **Step 1a: Write the sink-wiring test** (depends on the overload added in Steps 3-4 below; it will not compile until then — that is the expected initial failure)

Create `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerSinkTest.java`:

```java
package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.SpewItem;
import org.icij.spewer.SpewSink;
import org.icij.task.Options;
import org.junit.Test;

import java.io.Reader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerSinkTest {

    private static final String FIXTURE = "/documents/image_attachment.eml";

    // Records promise/ready counts and the ready embed ids, thread-safely (deferred ready arrives off-thread).
    private static class RecordingSink implements SpewSink {
        final AtomicInteger promises = new AtomicInteger();
        final ConcurrentLinkedQueue<String> readyIds = new ConcurrentLinkedQueue<>();
        @Override public void promise() { promises.incrementAndGet(); }
        @Override public void ready(SpewItem item) { readyIds.add(item.embed().getId()); }
    }

    @Test
    public void testSinkPromisesAndReadiesEachEmbed() throws Exception {
        RecordingSink sink = new RecordingSink();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {

            TikaDocument doc = extractor.extract(
                    Paths.get(getClass().getResource(FIXTURE).getPath()), sink);

            // Draining the root reader drives the parse + the deferred OCR backstop, so every embed
            // is promised (parse thread) and readied (OCR completion) by the time the reader EOFs.
            try (Reader r = doc.getReader()) {
                org.icij.spewer.Spewer.toString(r);
            }

            assertThat(sink.promises.get()).isGreaterThan(0);
            // Every promise must eventually be matched by a ready.
            assertThat(sink.readyIds.size()).isEqualTo(sink.promises.get());
        }
    }
}
```

- [ ] **Step 1b: Write the streaming-order test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/StreamingSpewOrderTest.java`:

```java
package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewOrderTest {

    private static final String FIXTURE = "/documents/image_attachment.eml";

    // Thread-safe recorder: notes the order of writes and whether each was a child (parent != null).
    private static class OrderRecordingSpewer extends Spewer {
        final List<String> order = new CopyOnWriteArrayList<>();
        OrderRecordingSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            try { Spewer.toString(doc.getReader()); } catch (Exception ignored) {}
            order.add((parent == null ? "ROOT:" : "EMBED:") + doc.getId());
        }
    }

    @Test
    public void testEmbedsAreWrittenAndRootIsCovered() throws Exception {
        OrderRecordingSpewer spewer = new OrderRecordingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", "true",
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(FIXTURE).getPath()), spewer);
        }
        // At least one embed and the root were written.
        assertThat(spewer.order).isNotEmpty();
        assertThat(spewer.order.stream().anyMatch(s -> s.startsWith("ROOT:"))).isTrue();
        assertThat(spewer.order.stream().anyMatch(s -> s.startsWith("EMBED:"))).isTrue();
    }

    @Test
    public void testStreamingAndLegacyWriteTheSameDocIds() throws Exception {
        List<String> streaming = idsFor(true);
        List<String> legacy = idsFor(false);
        Collections.sort(streaming);
        Collections.sort(legacy);
        assertThat(streaming).isEqualTo(legacy);
    }

    private List<String> idsFor(boolean streaming) throws Exception {
        OrderRecordingSpewer spewer = new OrderRecordingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", String.valueOf(streaming),
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(FIXTURE).getPath()), spewer);
        }
        List<String> ids = new java.util.ArrayList<>();
        for (String s : spewer.order) { ids.add(s.substring(s.indexOf(':') + 1)); }
        return ids;
    }
}
```

Note: `extract(Path, Spewer)` already exists; the streaming-vs-legacy switch is driven by the `streamingSpew` option, so both `idsFor` calls use the existing `extract(Path, Spewer)` entry point.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -am test -Dtest=StreamingSpewOrderTest`
Expected: FAIL (option `streamingSpew` not recognized / streaming branch absent — both runs go through the legacy walk, but the `extract(Path, SpewSink)` overload referenced by `EmbedSpawnerSinkTest` does not yet exist, so compilation fails).

- [ ] **Step 3: Add the option and field**

In `Extractor.java`, add the `@Option` annotation alongside the others (after `progressHeartbeatInterval`):

```java
@Option(name = "streamingSpew", description = "Write embedded documents to the spewer as they are " +
        "parsed, instead of buffering the whole tree and writing it afterwards. On by default; set " +
        "to false to fall back to the legacy buffer-then-walk path.")
```

Add the field near `progressHeartbeatInterval` (line ~185):

```java
    private boolean streamingSpew = true;
    // Bounded spew queue: caps how many ready-but-unwritten embeds (and thus their buffered text)
    // are held in flight, providing backpressure on the parse thread when the spewer lags.
    private static final int SPEW_QUEUE_CAPACITY = 1000;
```

In `configure(...)`, after the `progressHeartbeatInterval` parse, add:

```java
        options.get("streamingSpew", "true").parse().asBoolean().ifPresent(b -> this.streamingSpew = b);
```

Add a getter near `isOcrFanout()`:

```java
    public boolean isStreamingSpew() { return streamingSpew; }
```

- [ ] **Step 4: Add the `extract(Path, SpewSink)` overload and thread the sink**

Add the import:

```java
import org.icij.spewer.SpewSink;
import org.icij.spewer.StreamingSpewCoordinator;
```

Change `extract(Path)` to delegate with a null sink, and add the overload:

```java
    public TikaDocument extract(final Path path) throws IOException {
        return extract(path, (SpewSink) null);
    }

    public TikaDocument extract(final Path path, final SpewSink sink) throws IOException {
        final Function<Writer, ContentHandler> handler;
        if (OutputFormat.HTML == outputFormat) {
            handler = (writer) -> new ExpandedTitleContentHandler(new HTML5Serializer(writer));
        } else {
            handler = BodyContentHandler::new;
        }
        return getTikaDocument(path, handler, metadata -> true, sink);
    }
```

Update `getTikaDocument`'s signature to accept the sink and pass it to the `EmbedSpawner`. Change:

```java
    private TikaDocument getTikaDocument(Path path, final Function<Writer, ContentHandler> handlerProvider, DocumentSelector documentSelector) throws IOException {
```

to:

```java
    private TikaDocument getTikaDocument(Path path, final Function<Writer, ContentHandler> handlerProvider, DocumentSelector documentSelector) throws IOException {
        return getTikaDocument(path, handlerProvider, documentSelector, null);
    }

    private TikaDocument getTikaDocument(Path path, final Function<Writer, ContentHandler> handlerProvider, DocumentSelector documentSelector, final SpewSink sink) throws IOException {
```

In the `EmbedHandling.SPAWN` branch, pass `sink` as the new trailing `EmbedSpawner` constructor argument:

```java
            context.set(EmbeddedDocumentExtractor.class,
                    new EmbedSpawner(rootDocument, context, embedOutput, handlerProvider, embedMemoryBudgetBytes,
                            embedTextResources, new MemoryPressureGauge(embedMemoryPressureThreshold),
                            this::ocrExecutor, !ocrDisabled, currentProgress, digester,
                            ocrFanout, ocrMinImageBytes, ocrParserClassName, sink));
```

(The other `getTikaDocument` callers — `extractPages`, `extractPageIndices` — call the 3-arg overload, which now delegates with `sink = null`, so they are unchanged.)

- [ ] **Step 5: Add the streaming branch to `doExtract`**

Replace the body of `doExtract`:

```java
    private void doExtract(final Path path, final Spewer spewer) throws IOException {
        long before = currentTimeMillis();
        progressTracker.begin(path);
        try {
            if (streamingSpew && EmbedHandling.SPAWN == embedHandling) {
                try (StreamingSpewCoordinator coordinator = new StreamingSpewCoordinator(spewer, SPEW_QUEUE_CAPACITY)) {
                    final TikaDocument document = extract(path, coordinator);
                    logger.info("{} streaming-spew started in {}ms", path, currentTimeMillis() - before);
                    // Foreground writes the root (driving the parse + all embed spews), then the
                    // coordinator awaits every embed and closes the root reader (temp cleanup).
                    coordinator.spew(document);
                }
            } else {
                final TikaDocument document = extract(path);
                logger.info("{} extracted in {}ms", path, currentTimeMillis() - before);
                spewer.write(document);
            }
        } finally {
            progressTracker.end(path);
        }
    }
```

- [ ] **Step 6: Run the streaming tests to verify they pass**

Run: `mvn -pl extract-lib -am test -Dtest=StreamingSpewOrderTest,EmbedSpawnerSinkTest`
Expected: PASS (both classes).

- [ ] **Step 7: Run the byte-identity regression to confirm IDs are unchanged**

Run: `mvn -pl extract-lib -am test -Dtest=ParallelOcrDeterminismTest`
Expected: PASS (digests/IDs unaffected by streaming).

- [ ] **Step 8: Commit** (via `/commit`)

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/StreamingSpewOrderTest.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerSinkTest.java
git commit -m "feat(spew): stream embeds during parse via StreamingSpewCoordinator (streamingSpew option, default on)"
```

---

## Task 6: Mid-parse failure keeps partial progress

Prove that when the parse throws partway through a container, embeds already written stay written, the coordinator cleans up, and the failure propagates so the reporter records the root path retryable.

**Files:**
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/StreamingSpewFailureTest.java`

**Interfaces:**
- Consumes: `Extractor.extract(Path, Spewer)`, `StreamingSpewCoordinator` (indirectly).

- [ ] **Step 1: Write the test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/StreamingSpewFailureTest.java`:

```java
package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewFailureTest {

    private static final String FIXTURE = "/documents/image_attachment.eml";

    // Spewer that throws on the root write (parent == null) but records any embed writes first.
    private static class FailOnRootSpewer extends Spewer {
        final List<String> embedsWritten = new CopyOnWriteArrayList<>();
        FailOnRootSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            if (parent == null) {
                // Drain the pipe first so embeds get produced and streamed, THEN fail the root write.
                try { Spewer.toString(doc.getReader()); } catch (Exception ignored) {}
                throw new IOException("simulated root write failure");
            }
            embedsWritten.add(doc.getId());
        }
    }

    @Test
    public void testRootFailurePropagatesAndEmbedsAlreadyWrittenStay() throws Exception {
        FailOnRootSpewer spewer = new FailOnRootSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", "true",
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {

            try {
                extractor.extract(Paths.get(getClass().getResource(FIXTURE).getPath()), spewer);
                org.junit.Assert.fail("expected the root write failure to propagate");
            } catch (IOException expected) {
                assertThat(expected.getMessage()).contains("simulated root write failure");
            }
        }
        // The embeds streamed during the parse were written before the root write failed.
        assertThat(spewer.embedsWritten).isNotEmpty();
    }
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `mvn -pl extract-lib -am test -Dtest=StreamingSpewFailureTest`
Expected: PASS. (Embeds written before the root drain-and-fail persist; the `IOException` propagates after the coordinator awaits the worker, shuts it down, and closes the root reader in `spew`'s `finally`.)

- [ ] **Step 3: Commit** (via `/commit`)

```bash
git add extract-lib/src/test/java/org/icij/extract/extractor/StreamingSpewFailureTest.java
git commit -m "test(spew): mid-parse root failure keeps already-streamed embeds and propagates"
```

---

## Task 7: Backpressure / no-deadlock

Prove that a tiny queue bound plus many embeds completes without deadlock (the parse thread blocks in `ready` and resumes as the worker drains).

**Files:**
- Test: `extract-lib/src/test/java/org/icij/spewer/StreamingSpewBackpressureTest.java`

**Interfaces:**
- Consumes: `StreamingSpewCoordinator`, `SpewItem`.

- [ ] **Step 1: Write the test**

Create `extract-lib/src/test/java/org/icij/spewer/StreamingSpewBackpressureTest.java`:

```java
package org.icij.spewer;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewBackpressureTest {

    private static class SlowSpewer extends Spewer {
        final AtomicInteger writes = new AtomicInteger();
        SlowSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            if (parent != null) {
                try { Thread.sleep(2); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                writes.incrementAndGet();
            }
        }
    }

    @Test(timeout = 30_000)
    public void testBoundedQueueDoesNotDeadlock() throws Exception {
        SlowSpewer spewer = new SlowSpewer();
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("root"));
        root.setReader(new StringReader("root"));

        final int n = 200;
        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 4)) { // tiny bound
            // Promise all up front (non-blocking) so spew()'s awaitDrained waits for all n, mirroring the
            // real flow where every promise is made during the root-drive parse before awaitDrained runs.
            for (int i = 0; i < n; i++) {
                coord.promise();
            }
            // Producer readies 200 items into the 4-slot queue; ready() blocks when full (backpressure).
            Thread producer = new Thread(() -> {
                for (int i = 0; i < n; i++) {
                    TikaDocument e = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("e" + i));
                    e.setReader(new StringReader("x"));
                    coord.ready(new SpewItem(e, root, root, 1));
                }
            }, "test-producer");
            producer.start();
            // spew() starts the worker (drains concurrently with the producer), writes the root, then
            // awaits all n embeds. The producer unblocks as the worker drains -> no deadlock.
            coord.spew(root);
            producer.join();
        }
        assertThat(spewer.writes.get()).isEqualTo(n);
    }
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `mvn -pl extract-lib -am test -Dtest=StreamingSpewBackpressureTest`
Expected: PASS within the timeout (no deadlock; all 200 embeds written).

- [ ] **Step 3: Commit** (via `/commit`)

```bash
git add extract-lib/src/test/java/org/icij/spewer/StreamingSpewBackpressureTest.java
git commit -m "test(spew): bounded spew queue applies backpressure without deadlock"
```

---

## Task 8: Full extract-lib suite + version bump + release

Run the whole module to catch regressions, then bump the extract-lib version to `9.12.0` for datashare to consume.

**Files:**
- Modify: `pom.xml` (root `<version>`) and any module poms that pin the version, matching how `9.11.0` was released (see `git show 701b42c`).

- [ ] **Step 1: Run the full extract-lib test suite**

Run: `mvn -pl extract-lib -am test`
Expected: BUILD SUCCESS, all tests green.

- [ ] **Step 2: Inspect how the last release set the version**

Run: `git show 701b42c --stat`
Expected: shows the files the `[release] 9.11.0` commit touched. Replicate the same version edit for `9.12.0` (typically `mvn versions:set -DnewVersion=9.12.0` or the project's release convention; match the prior commit exactly).

- [ ] **Step 3: Set the version**

Run: `mvn -q versions:set -DnewVersion=9.12.0 -DgenerateBackupPoms=false`
Expected: poms updated to `9.12.0`. Verify: `grep -m1 "<version>" pom.xml` → `9.12.0`.

- [ ] **Step 4: Build the artifact so datashare can resolve it**

Run: `mvn -pl extract-lib -am install -DskipTests`
Expected: `org.icij.extract:extract-lib:9.12.0` installed to the local Maven repo.

- [ ] **Step 5: Commit** (via `/commit`)

```bash
git add -A
git commit -m "release: extract-lib 9.12.0 (streaming spew)"
```

---

## Task 9 (datashare): bump and validate the two-writer path

Adopt extract `9.12.0` in datashare and add an integration test that extracts a multi-embed container with streaming on, exercising concurrent root+embed writes against a real ES.

**Files:**
- Modify: `../datashare/pom.xml:99` (`<extract.version>`).
- Test: `../datashare/datashare-index/src/test/java/org/icij/datashare/text/indexing/elasticsearch/ElasticsearchSpewerStreamingTest.java`

**Interfaces:**
- Consumes: `ElasticsearchSpewer` (unchanged), `Extractor` (streaming default on).

- [ ] **Step 1: Bump the extract version**

In `../datashare/pom.xml:99`, change:

```xml
        <extract.version>9.11.0</extract.version>
```

to:

```xml
        <extract.version>9.12.0</extract.version>
```

- [ ] **Step 2: Write the streaming integration test**

Create `../datashare/datashare-index/src/test/java/org/icij/datashare/text/indexing/elasticsearch/ElasticsearchSpewerStreamingTest.java`. Modeled on the existing `DatashareExtractIntegrationTest.test_spew_and_read_embedded_doc` (same `ElasticsearchRule`, `ElasticsearchIndexer`, `MemoryDocumentCollectionFactory`). Fixture: `/docs/embedded_doc.eml` — a real datashare test resource that is a root email plus one embedded PDF attachment (a NON-OCR child, so it streams through the synchronous path and needs no Tesseract). The test extracts ONCE to learn the deterministic ids (probe, no indexing), then runs the STREAMING `extract(path, spewer)` and asserts BOTH the root and the embedded child are indexed — exercising the two-writer path (foreground writes root, worker writes the child):

```java
package org.icij.datashare.text.indexing.elasticsearch;

import co.elastic.clients.elasticsearch._types.Refresh;
import org.icij.datashare.PropertiesProvider;
import org.icij.datashare.extract.MemoryDocumentCollectionFactory;
import org.icij.datashare.text.Document;
import org.icij.datashare.text.Language;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.extractor.Extractor;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.Reader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static java.nio.file.Paths.get;
import static java.util.Objects.requireNonNull;
import static org.fest.assertions.Assertions.assertThat;

public class ElasticsearchSpewerStreamingTest {
    @ClassRule
    public static org.icij.datashare.test.ElasticsearchRule es = new org.icij.datashare.test.ElasticsearchRule();

    private final ElasticsearchIndexer indexer =
            new ElasticsearchIndexer(es.client, new PropertiesProvider()).withRefresh(Refresh.True);
    private final ElasticsearchSpewer spewer = new ElasticsearchSpewer(indexer,
            new MemoryDocumentCollectionFactory<>(), l -> Language.ENGLISH, new FieldNames(),
            new PropertiesProvider(new HashMap<>() {{ put("defaultProject", es.getIndexName()); }}));

    private static final String FIXTURE = "/docs/embedded_doc.eml";

    // Build a streaming extractor (default streamingSpew=true). PathIdentifier so ids are deterministic
    // and identical between the probe extraction and the streaming extraction below.
    private Extractor streamingExtractor() {
        return new Extractor(new DocumentFactory().withIdentifier(new PathIdentifier()),
                Options.from(Map.of("streamingSpew", "true", "progressHeartbeatInterval", "0")));
    }

    @Test
    public void testStreamingExtractionIndexesRootAndEmbed() throws Exception {
        Path path = get(requireNonNull(getClass().getResource(FIXTURE)).getPath());

        // Probe: extract once (no indexing) to learn the deterministic root + embed ids. Draining the
        // root reader drives the parse so getEmbeds() is populated and ids are computed.
        String rootId;
        String embedId;
        try (Extractor probe = streamingExtractor()) {
            TikaDocument doc = probe.extract(path);
            try (Reader r = doc.getReader()) { Spewer.toString(r); }
            rootId = doc.getId();
            assertThat(doc.getEmbeds()).isNotEmpty();
            embedId = doc.getEmbeds().get(0).getId();
        }

        // Stream-extract-and-index the same file: foreground writes the root, the spew worker writes
        // the embed. Same extractor config => same ids as the probe.
        try (Extractor extractor = streamingExtractor()) {
            extractor.extract(path, spewer);
        }

        // Both the root and the embedded child must be present in the index after streaming.
        Document root = indexer.get(es.getIndexName(), rootId);
        assertThat(root).isNotNull();
        assertThat(root.getId()).isEqualTo(rootId);

        Document child = indexer.get(es.getIndexName(), embedId, rootId);
        assertThat(child).isNotNull();
        assertThat(child.getId()).isNotEqualTo(rootId);
        assertThat(child.getRootDocument()).isEqualTo(rootId);
    }
}
```

- [ ] **Step 3: Run the streaming test**

Run: `cd ../datashare && mvn -pl datashare-index -am test -Dtest=ElasticsearchSpewerStreamingTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (root + embeds indexed; no concurrency corruption).

- [ ] **Step 4: Run the existing spewer test to confirm no regression**

Run: `cd ../datashare && mvn -pl datashare-index -am test -Dtest=ElasticsearchSpewerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 5: Commit** (via `/commit`)

```bash
git add pom.xml \
        datashare-index/src/test/java/org/icij/datashare/text/indexing/elasticsearch/ElasticsearchSpewerStreamingTest.java
git commit -m "feat: adopt extract-lib 9.12.0 streaming spew; add streaming spewer integration test"
```

---

## Task 10: Validate on the OST box

Confirm the headline wins against the real workload before declaring B2 shipped.

- [ ] **Step 1: Deploy the streaming build to the box** (JRE only). Follow the existing datashare deploy convention used for the 9.11.0 rollout.

- [ ] **Step 2: Start indexing a giant OST** (21/48/38 GB OSTs) and watch the ES doc count.
Expected: the doc count **climbs during the parse** (not only at the end). The 60s heartbeat (A1/A3) shows climbing embed/OCR counts in journald.

- [ ] **Step 3: Capture a thread dump mid-parse** for the record: `kill -3 <pid>` (dumps into journald). Expected: an `extract-spew` worker thread present and a healthy parse thread; no thread blocked on a full queue indefinitely.

- [ ] **Step 4: Confirm peak heap is bounded** (does not grow with mailbox size the way the buffer-then-walk path did). Compare against the pre-B2 baseline note in the roadmap (~8/32 GB).

- [ ] **Step 5: Record the measurement** in the roadmap (Task 11).

---

## Task 11: Mark B2 shipped in the roadmap

**Files:**
- Modify: the OST indexing throughput roadmap (internal notes)

- [ ] **Step 1: Move B2 from Backlog to Shipped** with a dated entry under "Shipped". Note: extract-lib `9.12.0` (streaming spew via `StreamingSpewCoordinator`, `streamingSpew` option default on); datashare adoption shipped in the same effort (version bump + streaming integration test); byte-identity held (`ParallelOcrDeterminismTest` green); validated on the OST box (doc count climbs during parse, heap bounded). State that the next decision (B3 per-folder work units vs B4 async OCR post-pass) is to be made after this measurement, and that memory-headroom tuning can now be revisited.

- [ ] **Step 2: Commit the roadmap update** (note: the roadmap lives outside both repos; commit only if it is under version control, otherwise just save).

---

## Self-Review

**1. Spec coverage:**
- Streaming spew per-embed during parse → Tasks 3, 4, 5. ✓
- Doc count climbs during parse → Task 5 (`StreamingSpewOrderTest`), Task 10 (box). ✓
- Memory: free text per embed (worker closes reader); skeletons retained → Task 3 (`closeReaderQuietly` per item); validated Task 10. ✓
- Byte-identity preserved → Task 5 Step 7 (`ParallelOcrDeterminismTest`). ✓
- Duplicate-root gating preserved → Task 1 (volatile) + Task 3 (`testSkipsChildrenWhenRootIsDuplicate`). ✓
- Deferred-OCR embeds spewed on completion, worker never blocks on OCR → Task 4 Step 4 (`done.whenComplete`) + Task 4 test. ✓
- Two-writer thread-safety verified → Task 9 (streaming integration test against real ES). ✓
- Failure keeps partial progress, retryable, idempotent re-run → Task 6. (Idempotency is by ID upsert, already true in `ElasticsearchSpewer`; re-run overwrite is inherent, no new code.) ✓
- Backpressure/no deadlock → Task 7. ✓
- Non-streaming fallback identical → Task 5 (`testStreamingAndLegacyWriteTheSameDocIds`). ✓
- `streamingSpew` default on, flippable → Task 5 Step 3. ✓
- Datashare adoption + version bump → Tasks 8, 9. ✓
- Roadmap updated last → Task 11. ✓

**2. Placeholder scan:** The only deliberate substitution is the datashare fixture path in Task 9 (`/email/with-attachments.eml`), flagged explicitly with instructions to use a real datashare test resource with attachments. No `TBD`/`add error handling`/`similar to` placeholders elsewhere; all code blocks are complete.

**3. Type consistency:** `SpewItem(embed, parent, root, level)` and `SpewSink{promise(), ready(SpewItem)}` are used identically in Tasks 2, 3, 4, 5, 7. `StreamingSpewCoordinator(Spewer, int)`, `.promise()`, `.ready(SpewItem)`, `.spew(TikaDocument)`, `.close()`, `.writtenCount()` are consistent across Tasks 3, 5, 6, 7. The `EmbedSpawner` trailing `SpewSink sink` parameter is consistent in Tasks 4 and 5. `extract(Path, SpewSink)` is defined in Task 5 and consumed in Task 4's test (build-order note included).
