package org.icij.extract.core;

import org.junit.*;

import java.nio.file.Paths;

public class ArrayPathQueueTest {

	@Test
	public void testCloseClearsTheQueue() {
		final PathQueue queue = ArrayPathQueue.create(1);

		Assert.assertEquals(0, queue.size());
		queue.add(Paths.get("essay.txt"));
		Assert.assertEquals(1, queue.size());

		queue.clear();
		Assert.assertEquals(0, queue.size());
	}
}
