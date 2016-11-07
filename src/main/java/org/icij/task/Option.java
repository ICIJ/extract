package org.icij.task;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Option<T, U, V> {

	Option<T, U, V> describe(final String description);

	String description();

	String name();

	Option<T, U, V> code(final Character code);

	Character code();

	String parameter();

	Option<T, U, V> parameter(final String parameter);

	Optional<V> value();

	V[] values();

	Option<T, U, V> update(final V[] values);

	Option<T, U, V> update(final V value);

	Option<T, U, V> update(final Supplier<List<V>> supplier);

	<R> Collection<R> values(final Function<U, R> parser);

	<R> Optional<R> value(final Function<U, R> parser);

	Optional<Duration> asDuration();

	Optional<Path> asPath();

	Optional<Integer> asInteger();

	Optional<Boolean> asBoolean();

	/**
	 * @return Returns {@literal true} if the toggle is explicitly set to on, otherwise returns {@literal false}.
	 */
	boolean on();

	/**
	 * @return Returns {@literal true} if the toggle is explicitly set to off, otherwise returns {@literal false}.
	 */
	boolean off();

	<E extends Enum<E>> Optional<E> asEnum(final Function<U, E> valueOf);

	abstract class Set<T extends Option> implements Iterable<T> {

		protected final Map<String, T> map = new HashMap<>();

		public T get(final String name) {
			return map.get(name);
		}

		public Set<T> add(final T option) {
			map.put(option.name(), option);
			return this;
		}

		abstract public T add(final String name);

		@Override
		public Iterator<T> iterator() {
			return new SetIterator<>(map);
		}
	}

	class SetIterator<T extends Option> implements Iterator<T> {

		private int cursor = 0;
		private final int length;
		private final Map<String, T> options;
		private final String[] keys;

		SetIterator(final Map<String, T> options) {
			length = options.size();
			keys = options.keySet().toArray(new String[length]);
			this.options = options;
		}

		@Override
		public boolean hasNext() {
			return cursor < length;
		}

		@Override
		public T next() {
			return options.get(keys[cursor++]);
		}

		@Override
		public void remove() {
			options.remove(keys[cursor]);
		}
	}
}
