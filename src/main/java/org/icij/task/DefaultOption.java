package org.icij.task;

import org.icij.time.HumanDuration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

public class DefaultOption implements Option<DefaultOption, String, String> {

	private final String name;
	private Character code = null;
	private String description = null;
	private String parameter = null;

	protected final LinkedList<String> values = new LinkedList<>();

	private DefaultOption(final String name) {
		this.name = name;
	}

	@Override
	public Optional<Duration> duration() {
		return value(HumanDuration::parse);
	}

	@Override
	public Optional<Path> path() {
		return value(Paths::get);
	}

	@Override
	public Optional<Integer> integer() {
		return value(Integer::valueOf);
	}

	@Override
	public Optional<Boolean> toggle() {
		return value(Boolean::valueOf);
	}

	@Override
	public boolean on() {
		final Optional<Boolean> toggle = toggle();

		return toggle.isPresent() && toggle.get();
	}

	@Override
	public boolean off() {
		final Optional<Boolean> toggle = toggle();

		return toggle.isPresent() && !toggle.get();
	}

	@Override
	public <E extends Enum<E>> Optional<E> set(final Function<String, E> valueOf) {
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
	public Optional<String> value() {
		return Optional.ofNullable(values.peek());
	}

	@Override
	public String[] values() {
		final String[] values = new String[this.values.size()];
		int i = 0;

		for (String value : this.values) {
			values[i++] = value;
		}

		return values;
	}

	@Override
	public synchronized DefaultOption update(final String value) {
		values.clear();
		values.add(value);

		return this;
	}

	@Override
	public synchronized DefaultOption update(String[] values) {
		this.values.clear();
		this.values.addAll(Arrays.asList(values));

		return this;
	}

	@Override
	public <R> Optional<R> value(final Function<String, R> parser) {
		final String value = values.peek();

		if (null != value) {
			return Optional.ofNullable(parser.apply(value));
		}

		return Optional.empty();
	}

	@Override
	public synchronized <R> Collection<R> values(final Function<String, R> parser) {
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
