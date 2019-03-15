package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public abstract class SQLCondition implements Condition {

	protected final FunctionalDataSource dataSource;

	public SQLCondition(final DataSource dataSource) {
		this.dataSource = FunctionalDataSource.cast(dataSource);
	}

	@Override
	public void await() throws InterruptedException {
		while (await(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
		}
	}

	@Override
	public boolean awaitUntil(final Date deadline) throws InterruptedException {
		return await(deadline.getTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public long awaitNanos(final long nanosTimeout) throws InterruptedException {
		final long now = System.nanoTime();

		await(nanosTimeout, TimeUnit.NANOSECONDS);
		return System.nanoTime() - now;
	}

	@Override
	public void awaitUninterruptibly() {
		while (true) {
			try {
				await();
				break;
			} catch (final InterruptedException ignored) {
			}
		}
	}
}
