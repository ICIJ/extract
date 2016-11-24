package org.icij.task;

import org.icij.time.HumanDuration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class StringOption implements Option<StringOption, String, String> {

	private static class DefaultSupplier implements Supplier<List<String>> {

		private final List<String> values = new LinkedList<>();

		@Override
		public List<String> get() {
			return values;
		}
	}

	private final String name;
	private Character code = null;
	private String description = null;
	private String parameter = null;

	protected Supplier<List<String>> values = new DefaultSupplier();

	public StringOption(final String name) {
		this.name = name;
	}

	@Override
	public Optional<Duration> asDuration() {
		return value(HumanDuration::parse);
	}

	@Override
	public Optional<Path> asPath() {
		return value(Paths::get);
	}

	@Override
	public Optional<Integer> asInteger() {
		return value(Integer::valueOf);
	}

	@Override
	public Optional<Boolean> asBoolean() {
		return value(value-> {
			value = value.toLowerCase(Locale.ROOT);
			if (value.equals("yes") || value.equals("on") || value.equals("true") || value.equals("1")) {
				return Boolean.TRUE;
			}

			return Boolean.FALSE;
		});
	}

	@Override
	public boolean on() {
		return asBoolean().orElse(Boolean.FALSE).equals(Boolean.TRUE);
	}

	@Override
	public boolean off() {
		return asBoolean().orElse(Boolean.TRUE).equals(Boolean.FALSE);
	}

	@Override
	public <E extends Enum<E>> Optional<E> asEnum(final Function<String, E> valueOf) {
		return value(valueOf);
	}

	@Override
	public StringOption code(final Character code) {
		this.code = code;
		return this;
	}

	@Override
	public StringOption describe(final String description) {
		this.description = description;
		return this;
	}

	@Override
	public String description() {
		return description;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public Character code() {
		return code;
	}

	@Override
	public String parameter() {
		return parameter;
	}

	@Override
	public StringOption parameter(final String parameter) {
		this.parameter = parameter;
		return this;
	}

	@Override
	public synchronized Optional<String> value() {
		final List<String> values = this.values.get();

		if (!values.isEmpty()) {
			return Optional.of(values.get(0));
		}

		return Optional.empty();
	}

	@Override
	public synchronized String[] values() {
		final List<String> values = this.values.get();

		return values.toArray(new String[values.size()]);
	}

	@Override
	public synchronized StringOption update(final String value) {
		final List<String> values = this.values.get();

		values.clear();
		values.add(value);

		return this;
	}

	@Override
	public synchronized StringOption update(final String[] values) {
		this.values.get().clear();
		this.values.get().addAll(Arrays.asList(values));

		return this;
	}

	@Override
	public synchronized StringOption update(final Supplier<List<String>> supplier) {
		values = supplier;

		return this;
	}

	@Override
	public synchronized <R> Optional<R> value(final Function<String, R> parser) {
		final List<String> values = this.values.get();

		if (values.isEmpty()) {
			return Optional.empty();
		}

		return Optional.of(parser.apply(values.get(0)));
	}

	@Override
	public synchronized <R> Collection<R> values(final Function<String, R> parser) {
		final List<String> values = this.values.get();
		final Collection<R> results = new ArrayList<>(values.size());

		for (String value : values) {
			results.add(parser.apply(value));
		}

		return results;
	}

}
