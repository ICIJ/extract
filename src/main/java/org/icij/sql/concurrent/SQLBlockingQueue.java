package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;

import javax.sql.DataSource;

public class SQLBlockingQueue<E> extends AbstractBlockingQueue<E> {

	private final DataSource ds;
	private final SQLBlockingQueueAdapter<E> adapter;

	public SQLBlockingQueue(final DataSource ds, final Lock lock, final SQLBlockingQueueAdapter<E> adapter) {
		super(lock);
		this.ds = ds;
		this.adapter = adapter;
	}

	@Override
	public int size() {
		try (final Connection c = ds.getConnection()) {
			return adapter.size(c);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public E peek() {
		try (final Connection c = ds.getConnection()) {
			return adapter.peek(c);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean contains(final Object o) {
		if (null == o) {
			throw new NullPointerException();
		}

		try (final Connection c = ds.getConnection()) {
			return adapter.contains(c, o);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean remove(final Object o) {
		if (null == o) {
			throw new NullPointerException();
		}

		try (final Connection c = ds.getConnection()) {
			return adapter.remove(c, o) > 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		try (final Connection c = ds.getConnection()) {
			adapter.clear(c);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public E poll() {
		try (final Connection c = ds.getConnection()) {
			return adapter.poll(c);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean add(final E element) {
		if (null == element) {
			throw new NullPointerException();
		}

		try (final Connection c = ds.getConnection()) {
			return adapter.add(c, element) > 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
