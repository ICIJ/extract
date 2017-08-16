package org.icij.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class Options<T> implements Iterable<Option<T>> {

	protected final Map<String, Option<T>> map = new HashMap<>();

	public <R> Optional<R> ifPresent(final String name, final Function<Option<T>, Optional<R>> function) {
		return map.containsKey(name) ? function.apply(get(name)) : Optional.empty();
	}

	public Option<T> get(final String name) {
		return map.get(name);
	}

	public Option<T> get(final Option<T> option) {
		return map.get(option.name());
	}

	public Options<T> add(final Option<T> option) {
		map.put(option.name(), option);
		return this;
	}

	public Option<T> add(final String name, final Function<Option<T>, OptionParser<T>> parser) {
		final Option<T> option = new Option<>(name, parser);

		add(option);
		return option;
	}

	public Option<T> add(final org.icij.task.annotation.Option option, final Function<Option<T>, OptionParser<T>>
			parser) {
		return add(option.name(), parser).describe(option.description())
				.parameter(option.parameter())
				.code(option.code());
	}

	public void add(final org.icij.task.annotation.OptionsClass optionsClass, final Function<Option<T>, OptionParser<T>>
			parser) {
		for (org.icij.task.annotation.Option option : optionsClass.value()
				.getAnnotationsByType(org.icij.task.annotation.Option.class)) {
			add(option, parser);
		}

		// Recursively import other Options from OptionsClass annotations.
		for (org.icij.task.annotation.OptionsClass otherClass : optionsClass.value()
				.getAnnotationsByType(org.icij.task.annotation.OptionsClass.class)) {
			add(otherClass, parser);
		}
	}

	@Override
	public Iterator<Option<T>> iterator() {
		return new OptionsIterator<>(map);
	}
}
