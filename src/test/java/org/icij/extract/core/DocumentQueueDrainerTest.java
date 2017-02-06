package org.icij.extract.core;

import org.icij.kaxxa.concurrent.BooleanSealableLatch;
import org.icij.kaxxa.concurrent.SealableLatch;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.queue.ArrayDocumentQueue;
import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.queue.DocumentQueueDrainer;
import org.icij.time.HumanDuration;
import org.junit.*;

import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

public class DocumentQueueDrainerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	private static class MockConsumer implements Consumer<Document> {

		private final Deque<Document> accepted = new ArrayDeque<>();

		@Override
		public void accept(final Document document) {
			accepted.add(document);
		}

		Deque<Document> getAccepted() {
			return accepted;
		}
	}

	private DocumentQueue createQueue() {
		final DocumentQueue queue = new ArrayDocumentQueue(26);

		for (char a = 'a'; a <= 'z'; a++) {
			queue.add(factory.create(Paths.get(Character.toString(a))));
		}

		return queue;
	}

	@Test
	public void testDefaultPollTimeoutIs0() {
		final DocumentQueue queue = createQueue();
		final Consumer<Document> consumer = new MockConsumer();
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer);

		Assert.assertEquals(0, drainer.getPollTimeout().getSeconds());
	}

	@Test
	public void testDrainsQueue() throws Throwable {
		final DocumentQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer);

		final long drained = drainer.drain().get();
		final Queue<Document> accepted = consumer.getAccepted();

		Assert.assertEquals(26, drained);
		Assert.assertEquals(26, accepted.size());
		Assert.assertEquals(0, queue.size());

		for (char a = 'a'; a <= 'z'; a++) {
			Assert.assertEquals(Character.toString(a), accepted.poll().toString());
		}
	}

	@Test
	public void testDrainsQueueUntilPoison() throws Throwable {
		final DocumentQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer);
		final Document poison = factory.create(Paths.get("c"));

		final long drained = drainer.drain(poison).get();
		final Queue<Document> accepted = consumer.getAccepted();

		Assert.assertEquals(2, drained);
		Assert.assertEquals(2, accepted.size());
		Assert.assertEquals("a", accepted.poll().toString());
		Assert.assertEquals("b", accepted.poll().toString());
		Assert.assertEquals(23, queue.size());
	}

	@Test
	public void testClearPollTimeout() throws Throwable {
		final DocumentQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer);
		final Document poison = factory.create(Paths.get("1"));

		drainer.clearPollTimeout();
		Assert.assertNull(drainer.getPollTimeout());

		// The drainer should wait indefinitely if the timeout is null.
		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				queue.add(factory.create(Paths.get("0")));
				queue.add(poison);
			}
		}, 1000);

		final long drained = drainer.drain(poison).get();
		final Deque<Document> accepted = consumer.getAccepted();

		Assert.assertEquals(27, drained);
		Assert.assertEquals(27, accepted.size());
		Assert.assertEquals(0, queue.size());
		Assert.assertEquals("0", accepted.getLast().toString());
	}

	@Test
	public void testSetPollTimeout() throws Throwable {
		final DocumentQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer);

		drainer.setPollTimeout(HumanDuration.parse("2s"));
		Assert.assertEquals(2, drainer.getPollTimeout().getSeconds());

		// The drainer should wait up to 2s.
		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				queue.add(factory.create(Paths.get("0")));
			}
		}, 1000);

		final long drained = drainer.drain().get();
		final Deque<Document> accepted = consumer.getAccepted();

		Assert.assertEquals(27, drained);
		Assert.assertEquals(27, accepted.size());
		Assert.assertEquals(0, queue.size());
		Assert.assertEquals("0", accepted.getLast().toString());
	}

	@Test
	public void testSetLatch() throws Throwable {
		final DocumentQueue queue = createQueue();
		final MockConsumer consumer = new MockConsumer();
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer);
		final SealableLatch latch = new BooleanSealableLatch();

		drainer.setLatch(latch);
		Assert.assertEquals(latch, drainer.getLatch());

		// The drainer should wait for a signal.
		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				try {
					queue.put(factory.create(Paths.get("0")));
					latch.signal();
					queue.put(factory.create(Paths.get("1")));
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Assert.fail(e.getMessage());
				}

				latch.seal();
				latch.signal();
			}
		}, 1000);

		final long drained = drainer.drain().get();
		final Deque<Document> accepted = consumer.getAccepted();

		Assert.assertEquals(28, drained);
		Assert.assertEquals(28, accepted.size());
		Assert.assertEquals(0, queue.size());
		Assert.assertEquals("1", accepted.pollLast().toString());
		Assert.assertEquals("0", accepted.pollLast().toString());

		drainer.clearLatch();
		Assert.assertNull(drainer.getLatch());
	}
}
