package org.icij.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * A latch class that is like a {@link CountDownLatch} except that it only requires a single signal to fire. Because a
 * latch is non-exclusive, it uses the shared acquire and release methods.
 *
 * @see <a href="http://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/AbstractQueuedSynchronizer.html">Example on AbstractQueuedSynchronizer Documentation</a>
 */
public class BooleanSealableLatch implements SealableLatch {

	private static class Sync extends AbstractQueuedSynchronizer {
		private static final long serialVersionUID = -5948076050343927307L;

		boolean isSignalled() { return getState() != 0; }

		@Override
		protected int tryAcquireShared(int ignore) {
			return isSignalled() ? 1 : -1;
		}

		@Override
		protected boolean tryReleaseShared(int ignore) {
			setState(1);
			return true;
		}
	}

	private final Sync sync = new Sync();

	volatile private boolean sealed = false;

	@Override
	public void signal() {
		sync.releaseShared(1);
	}

	@Override
	public void await() throws InterruptedException {
		// A sealed latch will never be signalled again, so there is nothing to wait for: return
		// immediately. Throwing here (as this used to) raced with callers that poll with
		// `while (!latch.isSealed()) latch.await();` -- the latch could be sealed in the gap between
		// the isSealed() check and this call, throwing IllegalStateException spuriously. Returning lets
		// such a loop re-check isSealed() and exit cleanly.
		if (sealed) {
			return;
		}
		sync.acquireSharedInterruptibly(1);
	}

	@Override
	public void seal() {
		sealed = true;
	}

	@Override
	public boolean isSealed() {
		return sealed;
	}
}
