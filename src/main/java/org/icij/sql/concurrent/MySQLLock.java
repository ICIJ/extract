package org.icij.sql.concurrent;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;

public class MySQLLock implements Lock {

	private final DataSource ds;
	private final String name;

	public MySQLLock(final DataSource ds, final String name) {
		if (name.getBytes(StandardCharsets.US_ASCII).length > 64) {
			throw new IllegalArgumentException("Lock names may not be longer than 64 characters.");
		}

		this.ds = ds;
		this.name = name;
	}

	@Override
	public void lock() {
		if (!lock(-1)) {
			throw new RuntimeException("Failed to get MySQL lock.");
		}
	}

	@Override
	public boolean tryLock() {
		return lock(0);
	}

	@Override
	public boolean tryLock(final long time, final TimeUnit unit) {
		return lock(unit.toSeconds(time));
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}

		lock();
	}

	@Override
	public void unlock() {
		try (final Connection c = ds.getConnection();
		final PreparedStatement q = c.prepareStatement("RELEASE_LOCK(?);")) {

			q.setString(1, name);
			q.execute();

			if (q.getUpdateCount() == 0) {
				throw new RuntimeException("The lock was not established by this thread.");
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Condition newCondition() {
		return new MySQLCondition();
	}

	private boolean lock(final long time) {
		try (final Connection c = ds.getConnection();
		final PreparedStatement q = c.prepareStatement("GET_LOCK(?, ?);")) {
			q.setString(1, name);
			q.setLong(2, time);
			q.execute();

			if (q.getUpdateCount() != 1) {
				return false;
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return true;
	}

	private class MySQLCondition implements Condition {

		@Override
		public void await() throws InterruptedException {
			while (await(Long.MAX_VALUE)) {
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}
		}

		@Override
		public void awaitUninterruptibly() {
			while (true) {
				try {
					await();
					break;
				} catch (InterruptedException ignored) {
				}
			}
		}

		@Override
		public boolean awaitUntil(final Date deadline) throws InterruptedException {
			return await(TimeUnit.MILLISECONDS.toNanos(deadline.getTime() - System.currentTimeMillis()));
		}

		@Override
		public long awaitNanos(final long nanosTimeout) throws InterruptedException {
			final long now = System.nanoTime();

			await(nanosTimeout);
			return System.nanoTime() - now;
		}

		@Override
		public boolean await(final long time, final TimeUnit unit) throws InterruptedException {
			return await(unit.toNanos(time));
		}

		@Override
		public void signal() {
			try (final Connection c = ds.getConnection()) {

				// Find a list of blocked threads to wake up.
				final List<Long> threads = findLockThreads(c, 1);

				if (!threads.isEmpty()) {
					killThread(c, threads.get(0));
				}

			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void signalAll() {
			try (final Connection c = ds.getConnection()) {

				// Find a list of blocked threads to wake up
				final List<Long> threads = findLockThreads(c, -1);

				for (Long thread : threads) {
					killThread(c, thread);
				}

			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		private void killThread(final Connection c, final long thread) throws SQLException {
			try (final PreparedStatement s = c.prepareStatement("KILL QUERY ?;")) {
				s.setLong(1, thread);
				s.execute();
			}
		}

		private List<Long> findLockThreads(final Connection c, final int limit) throws SQLException {
			final String listQuery = "SELECT Id, User, Host, Db, Command, Time, State, Info FROM " +
					"INFORMATION_SCHEMA.PROCESSLIST " +
					"WHERE STATE='User sleep' AND INFO REGEXP ? " +
					"ORDER BY TIME;";
			final List<Long> threads = new ArrayList<>();
			int i = 0;

			try (final PreparedStatement s = c.prepareStatement(listQuery)) {
				s.setString(1, "SELECT SLEEP\\([0-9]+\\), ");

				try (final ResultSet rs = s.executeQuery()) {

					// The first call to next makes the first row the current row.
					while ((i++ < limit || limit < 1) && rs.next()) {
						final String info = rs.getString(8);
						final Pattern pattern = Pattern.compile("SELECT SLEEP\\([\\d.]+\\), '(.*?)'");
						final Matcher matcher = pattern.matcher(info);

						if (matcher.find() && matcher.group(1).equals(name)) {
							threads.add(rs.getLong(1));
						}
					}
				}
			}

			return threads;
		}

		private boolean await(long nanosTimeout) throws InterruptedException {
			if (nanosTimeout <= 0) {
				return false;
			}

			final long now = System.nanoTime();

			try (final Connection c = ds.getConnection();
			final PreparedStatement s = c.prepareStatement("SELECT SLEEP(?), ?;")) {

				// Adjust timeout (due to time it took to get a connection).
				nanosTimeout -= System.nanoTime() - now;

				// Convert to seconds, but round to whole number of milliseconds.
				s.setFloat(1, Math.round(nanosTimeout / 1000000.0) / 1000f);
				s.setString(2, name);
				s.execute();

				try (final ResultSet rs = s.getResultSet()) {

					// The first call to next() positions the cursor on the first row.
					return !(null == rs || !rs.next()) && rs.getInt(1) == 0;

				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
