package org.icij.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class Options<T> implements Iterable<Option<T>> {

	protected final Map<String, Option<T>> map = new HashMap<>();

	public Option<T> get(final String name) {
		return map.get(name);
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

	@Override
	public Iterator<Option<T>> iterator() {
		return new OptionsIterator<>(map);
	}
}
