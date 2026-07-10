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
    private final AtomicLong writtenEmbeds = new AtomicLong();
    private final Object drainLock = new Object();
    private volatile Throwable workerError;
    private volatile Thread worker;
    // The root the streamed embeds belong to, captured off the worker as it writes them. It drives two
    // things: writeEarlyRootStub() makes the root visible in the index as soon as the first child is
    // written (during a long parse), and on close() after an aborted parse (the foreground never
    // reaches spew()) finalizeOrStubAbortedRoot() refreshes that stub's child count -- or, if no stub
    // was written, writes one -- so the streamed embeds are not orphaned. rootWritten is set once the
    // real root is written by spew(); rootStubWritten tracks whether this run's PARTIAL stub was written.
    private volatile TikaDocument seenRoot;
    private volatile boolean rootWritten;
    private volatile boolean rootStubWritten;
    private volatile boolean closed;

    public StreamingSpewCoordinator(final Spewer spewer, final int queueCapacity) {
        this.spewer = spewer;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
    }

    /**
     * Start the single spew-worker thread. Idempotent: a second call while a worker is running is a
     * no-op. This MUST be called before the parse begins producing embeds — i.e. before the
     * potentially long-blocking reader construction inside {@code Extractor.extract(path, sink)} — so
     * the worker drains the bounded queue WHILE the parse runs. If the worker only started after
     * {@code extract()} returned, a container whose root body text is emitted late (a PST/OST emits no
     * root text — all content is in embeds) would block {@code extract()}'s first-character read while
     * embeds fill the bounded queue with no consumer, deadlocking once the queue is full.
     */
    public synchronized void start() {
        if (worker != null) {
            return;
        }
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
            rootWritten = true;
        } catch (final Throwable t) {
            rootError = t;
        } finally {
            // Wait for all promised embeds (including in-flight deferred OCR) so the shared temp
            // resources are still alive while the worker reads them; only then close the root reader.
            awaitDrained();
            shutdownWorker();
            // The child count is final only now (the root was written before the worker drained,
            // because reading its content drives the parse). If the root was fully written and it is a
            // container (at least one child written), finalize it so the endpoint can record the child
            // count and mark it complete. Best-effort: a finalize failure must not mask the parse result.
            // Skip finalize when the thread was interrupted (parse cancelled): awaitDrained may have
            // returned early, so writtenEmbeds is not final, and a cancelled container must not be
            // recorded as a fully-processed one. The aborted-root path (finalizeOrStubAbortedRoot) covers
            // orphaned children instead.
            if (rootWritten && !root.isDuplicate() && writtenEmbeds.get() > 0
                    && !Thread.currentThread().isInterrupted()) {
                try {
                    spewer.finalizeRoot(root, writtenEmbeds.get());
                } catch (final Throwable t) {
                    logger.error("failed to finalize root {}", root.getId(), t);
                }
            }
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
                if (seenRoot == null) {
                    seenRoot = item.root();
                }
                try {
                    // Skip children of a duplicate root, matching the legacy tree walk's gating.
                    if (!item.root().isDuplicate()) {
                        spewer.writeDocument(item.embed(), item.parent(), item.root(), item.level());
                        writtenEmbeds.incrementAndGet();
                        // Only after the first child is durably indexed: a stub must never outlive a
                        // parse that wrote zero children (that would leave a contentless PARTIAL ghost
                        // root on abort). writeEarlyRootStub() is idempotent, so calling it per child is
                        // fine -- only the first call writes.
                        writeEarlyRootStub();
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

    /**
     * Write the container root as a contentless PARTIAL stub as soon as the first child is durably
     * indexed, so the root is visible in the index during a long parse instead of only at the end
     * (the foreground is blocked reading the root's content the whole time). Best-effort and once per
     * parse, on the worker thread. The end-of-parse real root write (same id+path) later overwrites the
     * stub with full content, and finalizeRoot marks it complete. A duplicate root needs no stub (its
     * children are skipped). The !rootWritten guard avoids clobbering an already-written real root in
     * the degenerate case where the root body was read before the first child was drained.
     */
    private void writeEarlyRootStub() {
        final TikaDocument root = seenRoot;
        if (rootStubWritten || rootWritten || root == null || root.isDuplicate()) {
            return;
        }
        try {
            rootStubWritten = spewer.writeRootStub(root, writtenEmbeds.get());
        } catch (final Throwable t) {
            logger.error("failed to write early root stub for {}", root.getId(), t);
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
        if (closed) {
            return; // idempotent: a second close() must not re-invoke the endpoint for the same root
        }
        closed = true;
        shutdownWorker();
        finalizeOrStubAbortedRoot();
    }

    /**
     * On close after an aborted parse (the foreground never completed the real root write), keep the
     * streamed children from being orphaned and record their count. If this run's early stub was
     * written, refresh its child count while keeping it PARTIAL; otherwise write the stub now (fallback
     * when the early stub failed, or when writeRootStub reported the root already existed so we must not
     * refresh a pre-existing complete root's count). Best-effort; runs after the worker drained, so
     * writtenEmbeds/seenRoot are final. A duplicate root or a root with no written children needs
     * nothing.
     */
    private void finalizeOrStubAbortedRoot() {
        if (rootWritten) {
            return; // the real root was written; the happy path already finalized it
        }
        final TikaDocument root = seenRoot;
        if (root == null || root.isDuplicate() || writtenEmbeds.get() == 0) {
            return;
        }
        try {
            if (rootStubWritten) {
                spewer.finalizeAbortedRoot(root, writtenEmbeds.get());
            } else {
                rootStubWritten = spewer.writeRootStub(root, writtenEmbeds.get());
            }
        } catch (final Throwable t) {
            logger.error("failed to finalize/stub aborted root {}", root.getId(), t);
        }
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
