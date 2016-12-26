package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.SQLException;

public interface SQLBlockingQueueAdapter<T> {

	/**
	 * Runs a query that deletes the specified item from the underlying table and return the number of rows deleted.
	 *
	 * @param o the object to delete
	 * @return the number of rows deleted
	 */
	int delete(final Connection c, final Object o) throws SQLException;

	/**
	 * Runs a query that deletes all the items in the queue table and returns the number deleted.
	 *
	 * @return the number of deleted elements
	 */
	int deleteAll(final Connection c) throws SQLException;

	/**
	 * Runs a query that checks whether the item exists in the underlying table.
	 *
	 * @param o the object to check
	 * @return whether the object is present in the queue table
	 */
	boolean exists(final Connection c, final Object o) throws SQLException;

	/**
	 * Counts the number of items in the queue table.
	 *
	 * @return the number of items
	 */
	int count(final Connection c) throws SQLException;

	/**
	 * Select the first item in the queue (from the head) and remove it.
	 *
	 * @return the first item in the queue, or {@code null} if the queue was empty
	 */
	T shift(final Connection c) throws SQLException;

	/**
	 * Add a item to the end (tail) of the queue.
	 *
	 * @param e the element to add
	 * @return the number of elements added
	 */
	int insert(final Connection c, final T e) throws SQLException;

	/**
	 * Select the first item in the table (the head) and return it.
	 *
	 * @return the first element in the queue
	 */
	T selectFirst(final Connection c) throws SQLException;
}
