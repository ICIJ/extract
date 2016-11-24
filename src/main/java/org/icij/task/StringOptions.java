package org.icij.task;

public class StringOptions extends Options<StringOption> {

	@Override
	public StringOption get(final String name) {
		return super.get(name);
	}

	@Override
	public StringOptions add(final StringOption option) {
		map.put(option.name(), option);
		return this;
	}

	@Override
	public StringOption add(final String name) {
		final StringOption option = new StringOption(name);

		add(option);
		return option;
	}
}
