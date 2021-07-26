package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.util.concurrent.locks.Lock;

public abstract class SQLLock implements Lock {

	protected final FunctionalDataSource dataSource;

	public SQLLock(final DataSource dataSource) {
		this.dataSource = new FunctionalDataSource(dataSource);
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}

		lock();
	}
}
