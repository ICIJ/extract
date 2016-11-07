package org.icij.task;

import org.icij.time.HumanDuration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class DefaultOption implements Option<DefaultOption, String, String> {

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

	private DefaultOption(final String name) {
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
		return value(Boolean::valueOf);
	}

	@Override
	public boolean on() {
		final Optional<Boolean> toggle = asBoolean();

		return toggle.isPresent() && toggle.get();
	}

	@Override
	public boolean off() {
		final Optional<Boolean> toggle = asBoolean();

		return toggle.isPresent() && !toggle.get();
	}

	@Override
	public <E extends Enum<E>> Optional<E> asEnum(final Function<String, E> valueOf) {
		return value(valueOf);
	}

	@Override
	public DefaultOption code(final Character code) {
		this.code = code;
		return this;
	}

	@Override
	public DefaultOption describe(final String description) {
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
	public DefaultOption parameter(final String parameter) {
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
	public synchronized DefaultOption update(final String value) {
		final List<String> values = this.values.get();

		values.clear();
		values.add(value);

		return this;
	}

	@Override
	public synchronized DefaultOption update(final String[] values) {
		this.values.get().clear();
		this.values.get().addAll(Arrays.asList(values));

		return this;
	}

	@Override
	public synchronized DefaultOption update(final Supplier<List<String>> supplier) {
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

	public static class Set extends Option.Set<DefaultOption> {

		@Override
		public DefaultOption get(final String name) {
			return super.get(name);
		}

		@Override
		public DefaultOption.Set add(final DefaultOption option) {
			map.put(option.name(), option);
			return this;
		}

		@Override
		public DefaultOption add(final String name) {
			final DefaultOption option = new DefaultOption(name);

			add(option);
			return option;
		}
	}
}
