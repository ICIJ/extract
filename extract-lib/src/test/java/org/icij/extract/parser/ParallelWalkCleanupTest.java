package org.icij.extract.parser;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.fest.assertions.Assertions.assertThat;

/**
 * cleanupParallelWalk must not close a handle a task is still using. We drive the cleanup ordering
 * directly: a task holds a "reading" flag until released; cleanup must wait for the task to finish
 * before the close step observes it. Uses a package-private hook (awaitQuietly) exercised via a
 * small test-visible entry point on the parser.
 */
public class ParallelWalkCleanupTest {

    @Test(timeout = 30_000)
    public void testCleanupAwaitsRunningTaskBeforeClosing() throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(1);
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean stillReading = new AtomicBoolean(true);
        final CountDownLatch release = new CountDownLatch(1);
        final Future<?> f = pool.submit(() -> {
            started.countDown();
            try { release.await(); } catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
            stillReading.set(false);
        });
        started.await();
        // Release shortly, then await: after awaitQuietly returns the task must have finished.
        new Thread(() -> { try { Thread.sleep(200); } catch (final InterruptedException ignored) {} release.countDown(); }).start();
        ResilientOutlookPSTParser.awaitQuietlyForTest(List.of(f));
        assertThat(stillReading.get()).isFalse();
        pool.shutdownNow();
    }
}
