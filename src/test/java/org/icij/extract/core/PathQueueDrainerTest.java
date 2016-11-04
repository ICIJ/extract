package org.icij.extract.core;

import org.icij.concurrent.BooleanSealableLatch;
import org.icij.concurrent.SealableLatch;
import org.icij.time.HumanDuration;
import org.junit.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

public class PathQueueDrainerTest {

	private static class MockConsumer implements Consumer<Path> {

		private final Deque<Path> accepted = new ArrayDeque<>();

		@Override
		public void accept(final Path path) {
			accepted.add(path);
		}

		Deque<Path> getAccepted() {
			return accepted;
		}
	}

	private PathQueue createQueue() {
		final PathQueue queue = ArrayPathQueue.create(26);

		for (char a = 'a'; a <= 'z'; a++) {
			queue.add(Paths.get(Character.toString(a)));
		}

		return queue;
	}

	@Test
	public void testDefaultPollTimeoutIs0() {
		final PathQueue queue = createQueue();
		final Consumer<Path> consumer = new MockConsumer();
		final PathQueueDrainer drainer = new PathQueueDrainer(queue, consumer);

		Assert.assertEquals(0, drainer.getPollTimeout().getSeconds());
	}

	@Test
	public void testDrainsQueue() throws Throwable {
		final PathQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final PathQueueDrainer drainer = new PathQueueDrainer(queue, consumer);

		final long drained = drainer.drain().get();
		final Queue<Path> accepted = consumer.getAccepted();

		Assert.assertEquals(26, drained);
		Assert.assertEquals(26, accepted.size());
		Assert.assertEquals(0, queue.size());

		for (char a = 'a'; a <= 'z'; a++) {
			Assert.assertEquals(Character.toString(a), accepted.poll().toString());
		}
	}

	@Test
	public void testDrainsQueueUntilPoison() throws Throwable {
		final PathQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final PathQueueDrainer drainer = new PathQueueDrainer(queue, consumer);
		final Path poison = Paths.get("c");

		final long drained = drainer.drain(poison).get();
		final Queue<Path> accepted = consumer.getAccepted();

		Assert.assertEquals(2, drained);
		Assert.assertEquals(2, accepted.size());
		Assert.assertEquals("a", accepted.poll().toString());
		Assert.assertEquals("b", accepted.poll().toString());
		Assert.assertEquals(23, queue.size());
	}

	@Test
	public void testClearPollTimeout() throws Throwable {
		final PathQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final PathQueueDrainer drainer = new PathQueueDrainer(queue, consumer);
		final Path poison = Paths.get("1");

		drainer.clearPollTimeout();
		Assert.assertNull(drainer.getPollTimeout());

		// The drainer should wait indefinitely if the timeout is null.
		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				queue.add(Paths.get("0"));
				queue.add(poison);
			}
		}, 1000);

		final long drained = drainer.drain(poison).get();
		final Deque<Path> accepted = consumer.getAccepted();

		Assert.assertEquals(27, drained);
		Assert.assertEquals(27, accepted.size());
		Assert.assertEquals(0, queue.size());
		Assert.assertEquals("0", accepted.getLast().toString());
	}

	@Test
	public void testSetPollTimeout() throws Throwable {
		final PathQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final PathQueueDrainer drainer = new PathQueueDrainer(queue, consumer);

		drainer.setPollTimeout(HumanDuration.parse("2s"));
		Assert.assertEquals(2, drainer.getPollTimeout().getSeconds());

		// The drainer should wait up to 2s.
		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				queue.add(Paths.get("0"));
			}
		}, 1000);

		final long drained = drainer.drain().get();
		final Deque<Path> accepted = consumer.getAccepted();

		Assert.assertEquals(27, drained);
		Assert.assertEquals(27, accepted.size());
		Assert.assertEquals(0, queue.size());
		Assert.assertEquals("0", accepted.getLast().toString());
	}

	@Test
	public void testSetLatch() throws Throwable {
		final PathQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final PathQueueDrainer drainer = new PathQueueDrainer(queue, consumer);
		final SealableLatch latch = new BooleanSealableLatch();

		drainer.setLatch(latch);
		Assert.assertEquals(latch, drainer.getLatch());

		// The drainer should wait for a signal.
		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				try {
					queue.put(Paths.get("0"));
					latch.signal();
					queue.put(Paths.get("1"));
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Assert.fail(e.getMessage());
				}

				latch.seal();
				latch.signal();
			}
		}, 1000);

		final long drained = drainer.drain().get();
		final Deque<Path> accepted = consumer.getAccepted();

		Assert.assertEquals(28, drained);
		Assert.assertEquals(28, accepted.size());
		Assert.assertEquals(0, queue.size());
		Assert.assertEquals("1", accepted.pollLast().toString());
		Assert.assertEquals("0", accepted.pollLast().toString());

		drainer.clearLatch();
		Assert.assertNull(drainer.getLatch());
	}
}
