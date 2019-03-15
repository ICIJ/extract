package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class MySQLLock extends SQLLock {

	private final String name;

	public MySQLLock(final DataSource dataSource, final String name) {
		super(dataSource);

		if (name.getBytes(StandardCharsets.US_ASCII).length > 64) {
			throw new IllegalArgumentException("Lock names may not be longer than 64 characters.");
		}

		this.name = name;
	}

	@Override
	public void lock() {
		if (!tryLock(-1, TimeUnit.SECONDS)) {
			throw new RuntimeException("Failed to get MySQL lock.");
		}
	}

	@Override
	public boolean tryLock() {
		return tryLock(0, TimeUnit.SECONDS);
	}

	@Override
	public boolean tryLock(final long time, final TimeUnit unit) {
		return dataSource.withStatementUnchecked("GET_LOCK(?, ?);", q -> {
			q.setString(1, name);
			q.setLong(2, unit.toSeconds(time));
			q.execute();

			return q.getUpdateCount() > 0;
		});
	}

	@Override
	public void unlock() {
		dataSource.withStatementUnchecked("RELEASE_LOCK(?);", q -> {
			q.setString(1, name);
			q.execute();

			if (q.getUpdateCount() == 0) {
				throw new RuntimeException("The lock was not established by this thread.");
			}
		});
	}

	@Override
	public Condition newCondition() {
		return new MySQLCondition(dataSource, name);
	}
}
