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
    // The root the streamed embeds belong to, captured off the worker as it writes them, so that on an
    // aborted parse (the foreground never reaches spew()) close() can still write a root stub and keep
    // those embeds from being orphaned. rootWritten is set once the real root is written by spew().
    private volatile TikaDocument seenRoot;
    private volatile boolean rootWritten;

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
        writeRootStubIfOrphaned();
    }

    /**
     * If the parse aborted before the foreground wrote the real root (spew() never ran to completion)
     * yet embeds were already streamed to the index, write a contentless root stub so those embeds are
     * not orphaned under a non-existent root. Best-effort and idempotent; runs after the worker drained,
     * so writtenEmbeds/seenRoot are final. A duplicate root needs no stub (its children were skipped).
     */
    private void writeRootStubIfOrphaned() {
        if (rootWritten) {
            return;
        }
        final TikaDocument root = seenRoot;
        if (root == null || root.isDuplicate() || writtenEmbeds.get() == 0) {
            return;
        }
        try {
            spewer.writeRootStub(root);
            rootWritten = true;
        } catch (final Throwable t) {
            logger.error("failed to write root stub for aborted parse of {}", root.getId(), t);
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
