package org.icij.task;

import org.icij.time.HumanDuration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

public class StringOptionParser implements OptionParser<String> {

	private final Option<String> option;

	public StringOptionParser(final Option<String> option) {
		this.option = option;
	}

	@Override
	public Optional<Duration> asDuration() {
		return option.value(HumanDuration::parse);
	}

	@Override
	public Optional<Path> asPath() {
		return option.value(Paths::get);
	}

	@Override
	public Optional<Integer> asInteger() {
		return option.value(Integer::valueOf);
	}

	@Override
	public Optional<Boolean> asBoolean() {
		return option.value(value-> {
			value = value.toLowerCase(Locale.ROOT);
			if (value.equals("yes") || value.equals("on") || value.equals("true") || value.equals("1")) {
				return Boolean.TRUE;
			}

			if (value.equals("no") || value.equals("off") || value.equals("false") || value.equals("0")) {
				return Boolean.FALSE;
			}

			throw new IllegalArgumentException(String.format("\"%s\" is not a valid value.", value));
		});
	}

	@Override
	public boolean isOn() {
		return asBoolean().orElse(Boolean.FALSE).equals(Boolean.TRUE);
	}

	@Override
	public boolean isOff() {
		return asBoolean().orElse(Boolean.TRUE).equals(Boolean.FALSE);
	}

	@Override
	public <E extends Enum<E>> Optional<E> asEnum(final Function<String, E> valueOf) {
		return option.value(valueOf);
	}
}
