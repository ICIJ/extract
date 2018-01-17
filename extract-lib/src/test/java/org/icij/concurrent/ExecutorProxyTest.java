package org.icij.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;

public class ExecutorProxyTest {
	@Test
	public void testShutdown() {
		final ExecutorProxyStub proxy = new ExecutorProxyStub();

		Assert.assertFalse(proxy.getExecutor().isShutdown());
		proxy.shutdown();
		Assert.assertTrue(proxy.getExecutor().isShutdown());
	}

	@Test
	public void testShutdownNow() {
		final ExecutorProxyStub proxy = new ExecutorProxyStub();

		Assert.assertFalse(proxy.getExecutor().isShutdown());
		proxy.shutdownNow();
		Assert.assertTrue(proxy.getExecutor().isShutdown());
	}

	@Test
	public void testAwaitTermination() throws ExecutionException, InterruptedException, TimeoutException {
		final ExecutorProxyStub proxy = new ExecutorProxyStub();

		final Future<Boolean> result = proxy.getExecutor().submit(()-> {
			Thread.sleep(1000);
			return true;
		});

		proxy.shutdown();
		Assert.assertTrue(proxy.awaitTermination(2, TimeUnit.SECONDS));
		Assert.assertTrue(result.get(0, TimeUnit.MILLISECONDS));

		Assert.assertTrue(proxy.awaitTermination(0, TimeUnit.SECONDS));
	}

	private class ExecutorProxyStub extends ExecutorProxy {
		ExecutorProxyStub() {
			super(Executors.newSingleThreadExecutor());
		}
		ExecutorService getExecutor() {
			return executor;
		}
	}
}
