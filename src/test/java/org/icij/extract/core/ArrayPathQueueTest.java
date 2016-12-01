package org.icij.extract.core;

import org.icij.extract.queue.ArrayPathQueue;
import org.icij.extract.queue.PathQueue;
import org.junit.*;

import java.nio.file.Paths;

public class ArrayPathQueueTest {

	@Test
	public void testCloseClearsTheQueue() {
		final PathQueue queue = new ArrayPathQueue(1);

		Assert.assertEquals(0, queue.size());
		queue.add(Paths.get("essay.txt"));
		Assert.assertEquals(1, queue.size());

		queue.clear();
		Assert.assertEquals(0, queue.size());
	}
}
