package org.icij.task.transformers;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.function.Function;

import org.icij.task.DefaultOption;

public class CommonsTransformer implements Function<DefaultOption.Set, Options> {

	@Override
	public Options apply(final DefaultOption.Set set) {
		final Options options = new Options();

		for (DefaultOption option : set) {
			options.addOption(Option.builder()
					.longOpt(option.name())
					.argName(option.parameter())
					.desc(option.description())
					.hasArg(true)
					.optionalArg(true)
					.build());
		}

		return options;
	}
}
