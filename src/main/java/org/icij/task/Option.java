package org.icij.task;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class Option<V> {

	private static class DefaultSupplier<V> implements Supplier<List<V>> {

		private final List<V> values = new LinkedList<>();

		@Override
		public List<V> get() {
			return values;
		}
	}

	private final String name;
	private Character code = null;
	private String description = null;
	private String parameter = null;

	protected Supplier<List<V>> values = new DefaultSupplier<>();
	protected final Function<Option<V>, OptionParser<V>> parser;

	public Option(final String name, final Function<Option<V>, OptionParser<V>> parser) {
		this.name = name;
		this.parser = parser;
	}

	public Option<V> code(final Character code) {
		this.code = code;
		return this;
	}

	public Option<V> describe(final String description) {
		this.description = description;
		return this;
	}

	public String description() {
		return description;
	}

	public String name() {
		return name;
	}

	public Character code() {
		return code;
	}

	public String parameter() {
		return parameter;
	}

	public Option<V> parameter(final String parameter) {
		this.parameter = parameter;
		return this;
	}

	public synchronized Optional<V> value() {
		final List<V> values = this.values.get();

		if (!values.isEmpty()) {
			return Optional.of(values.get(0));
		}

		return Optional.empty();
	}

	public synchronized List<V> values() {
		return this.values.get();
	}

	public synchronized Option<V> update(final V value) {
		final List<V> values = this.values.get();

		values.clear();
		values.add(value);

		return this;
	}

	public synchronized Option<V> update(final List<V> values) {
		this.values.get().clear();
		this.values.get().addAll(values);

		return this;
	}

	public synchronized Option<V> update(final Supplier<List<V>> supplier) {
		values = supplier;

		return this;
	}

	public synchronized <R> Optional<R> value(final Function<V, R> parser) {
		final List<V> values = this.values.get();

		if (values.isEmpty()) {
			return Optional.empty();
		}

		return Optional.of(parser.apply(values.get(0)));
	}

	public synchronized <R> Collection<R> values(final Function<V, R> parser) {
		final List<V> values = this.values.get();
		final Collection<R> results = new ArrayList<>(values.size());

		for (V value : values) {
			results.add(parser.apply(value));
		}

		return results;
	}

	public OptionParser<V> parse() {
		return parser.apply(this);
	}
}
