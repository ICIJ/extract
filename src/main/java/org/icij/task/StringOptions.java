package org.icij.task;

public class StringOptions extends Options<String> {

	@Override
	public Option<String> get(final String name) {
		return super.get(name);
	}

	@Override
	public Options<String> add(final Option<String> option) {
		map.put(option.name(), option);
		return this;
	}

	@Override
	public Option<String> add(final String name) {
		final StringOption option = new StringOption(name);

		add(option);
		return option;
	}
}
