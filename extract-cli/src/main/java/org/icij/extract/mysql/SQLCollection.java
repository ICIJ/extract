package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Iterator;

public abstract class SQLCollection<E> implements Collection<E> {

	protected final FunctionalDataSource source;
	final SQLCodec<E> codec;

	public SQLCollection(final DataSource source, final SQLCodec<E> codec) {
		this.source = FunctionalDataSource.cast(source);
		this.codec = codec;
	}

	@Override
	public boolean containsAll(final Collection<?> c) {
		for (Object o: c) {
			if (!this.contains(o)) {
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean addAll(final Collection<? extends E> c) {
		boolean changed = false;

		for (E o: c) {
			if (add(o)) {
				changed = true;
			}
		}

		return changed;
	}

	@Override
	public boolean removeAll(final Collection<?> c) {
		boolean changed = false;

		for (Object o: c) {
			if (remove(o) && !changed) {
				changed = true;
			}
		}

		return changed;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean changed = false;

		for (E e : this) {
			if (!c.contains(e)) {
				remove(e);
				changed = true;
			}
		}

		return changed;
	}

	@Override
	public Object[] toArray() {
		final Object[] a;

		final Iterator<E> iterator = iterator();
		final int size = this.size();
		int i = 0;

		a = new Object[size];

		while (i < size && iterator.hasNext()) {
			a[i++] = iterator.next();
		}

		return a;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {
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

		return a;
	}
}
