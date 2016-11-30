package org.icij.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class Options<T> implements Iterable<Option<T>> {

	protected final Map<String, Option<T>> map = new HashMap<>();

	public Option<T> get(final String name) {
		return map.get(name);
	}

	public Options<T> add(final Option<T> option) {
		map.put(option.name(), option);
		return this;
	}

	abstract public Option<T> add(final String name);

	@Override
	public Iterator<Option<T>> iterator() {
		return new OptionsIterator<>(map);
	}
}
