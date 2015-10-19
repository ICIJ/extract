package org.icij.extract.core;

import org.icij.extract.test.*;
import org.icij.extract.redis.Redis;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.redisson.Redisson;
import org.redisson.core.RMap;
import org.redisson.client.RedisConnectionException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Assume;

public class ReporterTest extends TestBase {

	@Test
	public void testSave() throws Throwable {
		final Redisson redisson = Redis.createClient();
		final RMap<String, Integer> report = Redis.getReport(redisson, "extract:test");
		final Reporter reporter = new Reporter(logger, report);

		final Path a = Paths.get("/path/to/a");
		final Path b = Paths.get("/path/to/b");

		try {
			reporter.save(a, 0);
		} catch (RedisConnectionException e) {
			Assume.assumeNoException(e);
			return;
		}

		Assert.assertEquals(0, reporter.status(a).intValue());
		Assert.assertTrue(reporter.succeeded(a));

		reporter.save(b, 1);
		Assert.assertEquals(1, reporter.status(b).intValue());
		Assert.assertFalse(reporter.succeeded(b));
	}
}
