package org.icij.task;

import java.util.Iterator;
import java.util.Map;

public class OptionsIterator<T> implements Iterator<Option<T>> {

	private int cursor = 0;
	private final int length;
	private final Map<String, Option<T>> options;
	private final String[] keys;

	OptionsIterator(final Map<String, Option<T>> options) {
		length = options.size();
		keys = options.keySet().toArray(new String[length]);
		this.options = options;
	}

	@Override
	public boolean hasNext() {
		return cursor < length;
	}

	@Override
	public Option<T> next() {
		return options.get(keys[cursor++]);
	}

	@Override
	public void remove() {
		options.remove(keys[cursor]);
	}
}
