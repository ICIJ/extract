package org.icij.task;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Option<V> {

	Option<V> describe(final String description);

	String description();

	String name();

	Option<V> code(final Character code);

	Character code();

	String parameter();

	Option<V> parameter(final String parameter);

	Optional<V> value();

	V[] values();

	Option<V> update(final V[] values);

	Option<V> update(final V value);

	Option<V> update(final Supplier<List<V>> supplier);

	<R> Collection<R> values(final Function<V, R> parser);

	<R> Optional<R> value(final Function<V, R> parser);

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

	<E extends Enum<E>> Optional<E> asEnum(final Function<V, E> valueOf);

}
