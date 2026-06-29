package org.icij.concurrent;

import org.junit.Test;

/**
 * A sealed latch will never be signalled again, so there is nothing left to wait for: {@link
 * BooleanSealableLatch#await()} must return immediately rather than throw or block.
 *
 * <p>Regression guard for a flaky {@code DocumentQueueDrainer} failure. The drainer polls with
 * {@code while (path == null && !latch.isSealed()) latch.await();}. await() used to throw
 * {@code IllegalStateException("The latch is sealed.")} when the latch was already sealed, which
 * raced with that loop: if the producer sealed the latch in the gap between the {@code isSealed()}
 * check and the {@code await()} call, await() threw and the exception surfaced from
 * {@code drain().get()}. Returning instead lets the loop re-check {@code isSealed()} and exit cleanly.
 */
public class BooleanSealableLatchTest {

    @Test(timeout = 2_000)
    public void testAwaitOnSealedLatchReturnsImmediatelyWithoutThrowing() throws InterruptedException {
        final BooleanSealableLatch latch = new BooleanSealableLatch();
        latch.seal();
        latch.await(); // must return; previously threw IllegalStateException("The latch is sealed.")
    }

    @Test(timeout = 2_000)
    public void testAwaitReturnsAfterSignal() throws InterruptedException {
        final BooleanSealableLatch latch = new BooleanSealableLatch();
        latch.signal();
        latch.await(); // already signalled -> returns immediately (normal path, unchanged)
    }
}
