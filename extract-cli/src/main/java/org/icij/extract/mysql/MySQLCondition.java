package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class MySQLCondition extends SQLCondition {

	private final String name;

	public MySQLCondition(final DataSource dataSource, final String name) {
		super(dataSource);
		this.name = name;
	}

	@Override
	public void signal() {
		dataSource.withConnectionUnchecked(c -> {

			// Find one blocked thread to wake up.
			final List<Long> threads = findLockThreads(c, 1);

			if (!threads.isEmpty()) {
				killThread(c, threads.get(0));
			}
		});
	}

	@Override
	public void signalAll() {
		dataSource.withConnectionUnchecked(c -> {

			// Find a list of blocked threads to wake up.
			final List<Long> threads = findLockThreads(c, -1);

			for (Long thread : threads) {
				killThread(c, thread);
			}
		});
	}

	@Override
	public boolean await(final long time, final TimeUnit unit) throws InterruptedException {
		final long nanosTimeout = unit.toNanos(time);

		if (nanosTimeout <= 0) {
			return false;
		}

		final long now = System.nanoTime();

		return dataSource.withStatementUnchecked("SELECT SLEEP(?), ?;", s -> {

			// Adjust timeout (due to time it took to get a connection).
			final long adjustedTimeout = nanosTimeout - System.nanoTime() - now;

			// Convert to seconds, but round to whole number of milliseconds.
			s.setFloat(1, Math.round(adjustedTimeout / 1000000.0) / 1000f);
			s.setString(2, name);
			s.execute();

			try (final ResultSet rs = s.getResultSet()) {

				// The first call to next() positions the cursor on the first row.
				return !(null == rs || !rs.next()) && rs.getInt(1) == 0;

			}
		});
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
}
