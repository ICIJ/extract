package org.icij.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class Options<T extends Option> implements Iterable<T> {

	protected final Map<String, T> map = new HashMap<>();

	public T get(final String name) {
		return map.get(name);
	}

	public Options<T> add(final T option) {
		map.put(option.name(), option);
		return this;
	}

	abstract public T add(final String name);

	@Override
	public Iterator<T> iterator() {
		return new OptionsIterator<>(map);
	}
}
