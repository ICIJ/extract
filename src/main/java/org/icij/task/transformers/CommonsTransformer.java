package org.icij.task.transformers;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.function.Function;

import org.icij.task.StringOption;
import org.icij.task.StringOptions;

public class CommonsTransformer implements Function<StringOptions, Options> {

	@Override
	public Options apply(final StringOptions options) {
		final Options commonsOptions = new Options();

		for (StringOption option : options) {
			String code = option.code() == null ? null : option.code().toString();

			// The DefaultParser in commons-cli clones option objects before updating the value.
			// Work around this by overriding the clone method.
			Option commonsOption = new Option(code, option.name(), true, option.description()) {

				private static final long serialVersionUID = 7104410298761951462L;

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
