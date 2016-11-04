package org.icij.task.transformers;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.function.Function;

import org.icij.task.DefaultOption;

public class CommonsTransformer implements Function<DefaultOption.Set, Options> {

	@Override
	public Options apply(final DefaultOption.Set options) {
		final Options commonsOptions = new Options();

		for (DefaultOption option : options) {
			String code = option.code() == null ? null : option.code().toString();

			// The DefaultParser in commons-cli clones option objects before updating the value.
			// Work around this by overriding the clone method.
			Option commonsOption = new Option(code, option.name(), true, option.description()) {

				@Override
				public Option clone() {
					return this;
				}
			};

			commonsOption.setArgName(option.parameter());
			commonsOption.setOptionalArg(true);
			option.update(commonsOption::getValuesList);
			commonsOptions.addOption(commonsOption);
		}

		return commonsOptions;
	}
}
