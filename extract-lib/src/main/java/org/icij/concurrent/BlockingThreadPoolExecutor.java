package org.icij.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a specialized extension of the {@link ThreadPoolExecutor} class.
 *
 * It is based on a version originally written by Yaneeve Shekel and Amir Kirsh with the difference that a counting
 * {@link Semaphore} is used instead of a {@link RejectedExecutionHandler}, which makes for a simpler implementation.
 *
 * Two functions had been added to this subclass.
 *
 * 1) The execute method of the {@code ThreadPoolExecutor} will block in case the queue is full and only unblock when
 * the queue is de-queued - that is a task that is currently in the queue is removed and handled by the {@code
 * ThreadPoolExecutor}.
 *
 * 2) Client code can await for the event of all tasks being run to conclusion. Client code which actively chooses to
 * wait for this occurrence should call {@link BlockingThreadPoolExecutor#await} on the instance of this
 * {@code ThreadPoolExecutor}. This differs from {@link ThreadPoolExecutor#awaitTermination} as it does not require
 * any call to {@link ThreadPoolExecutor#shutdown} and may be used repeatedly.
 *
 * Code example:
 *
 * <code>
 * BlockingThreadPoolExecutor threadPoolExecutor = new BlockingThreadPoolExecutor(5);
 *
 * for (int i = 0; i < 5000; i++) {
 *   threadPoolExecutor.execute(...)
 * }
 *
 * try {
 *   threadPoolExecutor.await();
 * } catch (InterruptedException e) {
 *
 *   // Handle error.
 * }
 *
 * System.out.println("Done!");
 * </code>
 *
 * The example above shows how 5000 tasks are run within five threads. The line with {@code System.out.println
 * ("Done!");} will not run until such a time when all the tasks given to the thread pool have concluded their run.
 *
 * @see <a href="https://community.oracle.com/docs/DOC-983726">Creating a NotifyingBlockingThreadPoolExecutor Blog</a>
 */
public class BlockingThreadPoolExecutor extends ThreadPoolExecutor {

	/**
	 * The maximum amount of time to block while waiting for a permit.
	 */
	private final long maximumBlockingTime;

	/**
	 * The unit of time to block before rejecting execution.
	 */
	private final TimeUnit maximumBlockingUnit;

	/**
	 * Counts the number of current tasks in process.
	 */
	private final AtomicInteger tasksInProcess = new AtomicInteger();

	/**
	 * A semaphore that counts the number of available permits, that is, the number of tasks which may be queued.
	 */
	private final Semaphore permits;

	/**
	 * This is the {@code Synchronizer} instance that is used in order to notify all interested code of when all the
	 * tasks that have been submitted to the {@link #execute(Runnable)} method have run to conclusion. This
	 * notification can occur a numerous amount of times. It is all up to the client code. Whenever the {@code
	 * ThreadPoolExecutor} concludes to run all the tasks the {@code Synchronizer} object will be notified and will
	 * in turn notify the code which is waiting on it.
	 */
	private final Synchronizer synchronizer = new Synchronizer();

	/**
	 * This constructor is used in order to maintain the first functionality specified above. It does so by using an
	 * ArrayBlockingQueue and the BlockThenRunPolicy that is defined in this class. This constructor allows to give a
	 * timeout for the wait on new task insertion and to react upon such a timeout if occurs.
	 *
	 * @param corePoolSize corePoolSize the number of threads to keep in the pool, even if they are idle, unless
	 * {@code allowCoreThreadTimeOut} is set
	 * @param maximumPoolSize the maximum number of threads to allow in the pool
	 * @param keepAliveTime when the number of threads is greater than the core, this is the maximum time that excess
	 *                         idle threads will wait for new tasks before terminating.
	 * @param unit the time unit for the {@code keepAliveTime} argument
	 * @param handler the handler to use when execution is blocked because the thread bounds and queue capacities are
	 *                   reached
	 * @param threadFactory the factory to use when the executor creates a new thread
	 * @param maximumBlockingTime is the maximum time to wait on the queue of tasks before throwing a {@code
	 * RejectedExecutionException}, or {@code 0} to wait indefinitely
	 * @param maximumBlockingUnit is the unit of time to use with the previous parameter
	 */
	public BlockingThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long
			keepAliveTime, final TimeUnit unit, final BlockingQueue<Runnable> workQueue, final ThreadFactory
			threadFactory, final RejectedExecutionHandler handler, final long maximumBlockingTime, final TimeUnit
			                                           maximumBlockingUnit) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
		this.permits = new Semaphore(maximumPoolSize);
		this.maximumBlockingTime = maximumBlockingTime;
		this.maximumBlockingUnit = maximumBlockingUnit;
	}

	public BlockingThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long
			keepAliveTime, final TimeUnit unit, final BlockingQueue<Runnable> workQueue) {
		this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, Executors.defaultThreadFactory(), new
				AbortPolicy(), 0L, TimeUnit.MILLISECONDS);
	}

	public BlockingThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long
			keepAliveTime, final TimeUnit unit) {
		this(corePoolSize, maximumPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<>());
	}

	public BlockingThreadPoolExecutor(final int poolSize) {
		this(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS);
	}


	/**
	 * Before calling super's version of this method, a permit is acquired in order to queue the task for execution.
	 * This prevents the queue from being flooded with more tasks than the pool can execute at one go.
	 *
	 * @see ThreadPoolExecutor#execute(Runnable)
	 * @throws RejectedExecutionException if the thread is interrupted while waiting for a slot to become available
	 * to execute a task.
	 */
	@Override
	public void execute(final Runnable task) {
		try {
			if (maximumBlockingTime > 0) {
				if (!permits.tryAcquire(maximumBlockingTime, maximumBlockingUnit)) {
					throw new RejectedExecutionException(String.format("Unable to acquire a permit in %d %s.",
							maximumBlockingTime, maximumBlockingUnit));
				}
			} else {
				permits.acquire();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RejectedExecutionException("The thread was interrupted before task could be submitted for " +
					"execution.",	e);
		}

		// When a permit is acquired successfully, count a new task in process.
		tasksInProcess.incrementAndGet();

		try {
			super.execute(task);
		} catch (RejectedExecutionException e) {
			tasksInProcess.decrementAndGet();
			permits.release();
			throw e;
		}
	}

	/**
	 * After calling super's implementation of this method, the number of acquired permits is decremented. Finally, if
	 * the amount of tasks currently running is zero the synchronizer's {@code signalAll()} method is invoked, thus
	 * anyone awaiting on this instance of ThreadPoolExecutor is released.
	 *
	 * @see ThreadPoolExecutor#afterExecute(Runnable, Throwable)
	 */
	@Override
	protected void afterExecute(final Runnable r, final Throwable t) {
		super.afterExecute(r, t);

		// Synchronize on the pool (and actually all its threads). The synchronization is needed to avoid more than
		// one signal if two or more threads decrement almost together and come to the if with 0 tasks together.
		synchronized (this) {
			tasksInProcess.decrementAndGet();
			permits.release();
			if (tasksInProcess.intValue() == 0) {
				synchronizer.signalAll();
			}
		}
	}

	/**
	 * Increase or decreases the maximum pool size by adjusting the number of permits accordingly. This method will
	 * block (up till the maximum blocking time, if specified) when decreasing the pool size, until the number of
	 * threads is reduced to the desired size, but may be interrupted.
	 *
	 * @throws RuntimeException if the pool size could not be decreased within the maximum blocking time.
	 * @see ThreadPoolExecutor#setMaximumPoolSize(int)
	 */
	@Override
	public void setMaximumPoolSize(int maximumPoolSize) {
		if (maximumPoolSize <= 0) {
			throw new IllegalArgumentException("Maximum pool size must be greater than zero.");
		}

		synchronized (this) {
			int delta = maximumPoolSize - getMaximumPoolSize();

			// Increase the number of permits.
			if (delta > 0) {
				permits.release(delta - tasksInProcess.get());

			// Decrease the number of permits by acquiring the difference and never releasing.
			} else if (delta < 0) {
				delta = Math.abs(delta);

				try {
					if (maximumBlockingTime > 0) {
						if (!permits.tryAcquire(delta, maximumBlockingTime, maximumBlockingUnit)) {
							throw new RuntimeException(String.format("Unable to acquire %d permits in %d %s.", delta,
									maximumBlockingTime, maximumBlockingUnit));
						}
					} else {
						permits.acquire(delta);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}

			super.setMaximumPoolSize(maximumPoolSize);
		}
	}

	/**
	 * A blocking wait for this {@code ThreadPoolExecutor} to be in idle state, which means that there are no more tasks
	 * in the {@code Queue} or currently executed by one of the threads. BE AWARE that this method may get out from
	 * blocking state when a task is currently sent to the {@code ThreadPoolExecutor} not from this thread context.
	 * Thus it is not safe to call this method in case there are several threads feeding the {@code
	 * ThreadPoolExecutor} with tasks (calling {@code execute}). The safe way to call this method is from the thread
	 * that is calling {@code execute} and when there is only one such thread. Note that this method differs from
	 * {@code awaitTermination, as it can be called without shutting down the {@code ThreadPoolExecutor}.
	 *
	 * @throws InterruptedException when the internal condition throws it.
	 */
	public void await() throws InterruptedException {
		synchronizer.await();
	}

	/**
	 * A blocking wait for this ThreadPool to be in idle state or a certain timeout to elapse. Works the same as the
	 * {@code await()} method, except for adding the timeout condition.
	 *
	 * @return {@code false} if the timeout elapsed, {@code true} if the synchronous event we are waiting for had
	 * happened
	 * @throws InterruptedException when the internal condition throws it.
	 * @see BlockingThreadPoolExecutor#await() for more details.
	 */
	public boolean await(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
		return synchronizer.await(timeout, timeUnit);
	}

	/**
	 * This inner class serves to notify all interested parties that the {@code ThreadPoolExecutor} has finished running
	 * all the tasks given to its execute method.
	 */
	private class Synchronizer {

		private final Lock lock = new ReentrantLock();
		private final Condition done = lock.newCondition();
		private boolean isDone = false;

		/**
		 * This method allows the {@code ThreadPoolExecutor} to notify all interested parties that all tasks given to
		 * the execute method have run to conclusion.
		 */
		private void signalAll() {
			lock.lock(); // Must lock.
			try {
				isDone = true; // To help the await method ascertain that it has not waken up 'spuriously'.
				done.signalAll();
			} finally {
				lock.unlock(); // Make sure to unlock even in case of an exception.
			}
		}

		/**
		 * This is the inner implementation for supporting the {@code BlockingThreadPoolExecutor.await()}.
		 *
		 * @throws InterruptedException when the internal condition throws it.
		 * @see BlockingThreadPoolExecutor#await() for details.
		 */
		public void await() throws InterruptedException {
			lock.lock(); // Must lock.
			try {
				while (!isDone) { // Ascertain that this is not a 'spurious wake-up'.
					done.await();
				}
			} finally {
				isDone = false; // For next time.
				lock.unlock(); // Make sure to unlock even in case of an exception.
			}
		}

		/**
		 * This is the inner implementation for supporting the BlockingThreadPoolExecutor.await(timeout,
		 * timeUnit).
		 *
		 * @throws InterruptedException when the internal condition throws it.
		 * @see BlockingThreadPoolExecutor#await(long, TimeUnit) for details.
		 */
		public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
			boolean result = false;
			boolean isDone;

			lock.lock(); // Must lock.
			try {
				result = done.await(timeout, timeUnit);
			} finally {
				isDone = this.isDone;
				this.isDone = false; // For next time.
				lock.unlock(); // Make sure to unlock even in case of an exception.
			}

			// Make sure we return true only if done.
			return result && isDone;
		}
	}
}
