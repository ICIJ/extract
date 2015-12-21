package org.icij.extract.core;

import org.icij.extract.test.*;
import org.icij.extract.redis.RedisReport;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.redisson.client.RedisConnectionException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Assume;

public class ReporterTest extends TestBase {

	@Test
	public void testSave() throws Throwable {
		final Path a = Paths.get("/path/to/a");
		final Path b = Paths.get("/path/to/b");

		try {
			final Report report = RedisReport.create("extract:report:test");
			final Reporter reporter = new Reporter(report);

			reporter.save(a, ReportResult.SUCCEEDED);
			Assert.assertTrue(reporter.check(a, ReportResult.SUCCEEDED));
			reporter.save(b, ReportResult.NOT_FOUND);
			Assert.assertTrue(reporter.check(b, ReportResult.NOT_FOUND));
			Assert.assertFalse(reporter.check(b, ReportResult.SUCCEEDED));
		} catch (RedisConnectionException|IllegalStateException e) {
			Assume.assumeNoException(e);
		}
	}
}
