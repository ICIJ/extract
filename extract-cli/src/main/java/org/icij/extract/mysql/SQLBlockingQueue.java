package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public abstract class SQLBlockingQueue<E> extends SQLCollection<E> implements BlockingQueue<E> {

	private final Lock lock;
	private final Condition notEmpty;

	SQLBlockingQueue(final DataSource dataSource, final SQLCodec<E> codec, final Lock lock) {
		super(dataSource, codec);
		this.lock = lock;
		this.notEmpty = lock.newCondition();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean offer(final E e) {
		return add(e);
	}

	@Override
	public E element() {
		final E head = peek();

		if (head == null) {
			throw new NoSuchElementException();
		}

		return head;
	}

	@Override
	public E remove() {
		final E head = poll();

		if (head == null) {
			throw new NoSuchElementException();
		}

		return head;
	}

	@Override
	public E take() throws InterruptedException {
		E head = null;

		// We loop around trying to get a item, blocking at most a minute at a time this allows us to be interrupted.
		while (head == null) {
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			head = poll(1, TimeUnit.MINUTES);
		}

		return head;
	}

	@Override
	public void put(final E e) throws InterruptedException {
		add(e);
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		final Date deadline = new Date(System.currentTimeMillis() + unit.toMillis(timeout));

		E head = null;
		boolean stillWaiting = true;

		while (stillWaiting) {

			// Check if we can grab one.
			head = poll();
			if (head != null)
				break;

			// Block until we are woken, or deadline.
			// Because we don't have a distributed lock around this condition, there is a race condition
			// whereby we might miss a notify(). However, we can somewhat mitigate the problem, by using
			// this in a polling fashion.
			stillWaiting = notEmpty.awaitUntil(deadline);
		}

		return head;
	}

	@Override
	public int drainTo(final Collection<? super E> c) {
		return drainTo(c, Integer.MAX_VALUE);
	}


	@Override
	public int drainTo(final Collection<? super E> c, final int maxElements) {
		if (c == this) {
			throw new IllegalArgumentException("Draining to self is not supported.");
		}

		final Lock lock = this.lock;
		int i = 0;

		lock.lock();

		try {
			final Iterator<E> iterator = iterator();

			while (i < maxElements && iterator.hasNext()) {
				c.add(iterator.next());
				i++;
			}
		} finally {
			lock.unlock();
		}

		return maxElements - i;
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean offer(final E e, final long timeout, final TimeUnit unit) throws InterruptedException {

		// Right now, we have no concept of a full queue, so we don't block on insert.
		return offer(e);
	}

	@Override
	public Object[] toArray() {
		final Object[] a;
		final Lock lock = this.lock;

		lock.lock();

		try {
			final Iterator<E> iterator = iterator();
			final int size = this.size();
			int i = 0;

			a = new Object[size];

			while (i < size && iterator.hasNext()) {
				a[i++] = iterator.next();
			}
		} finally {
			lock.unlock();
		}

		return a;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {
		final Lock lock = this.lock;

		lock.lock();

		try {
			final Iterator<E> iterator = iterator();
			final int size = this.size();
			final int length = a.length;
			int i = 0;

			if (length < size) {
				a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
			}

			while (i < size && iterator.hasNext()) {
				a[i++] = (T) iterator.next();
			}

			if (length > size) {
				a[size] = null;
			}
		} finally {
			lock.unlock();
		}

		return a;
	}
}
