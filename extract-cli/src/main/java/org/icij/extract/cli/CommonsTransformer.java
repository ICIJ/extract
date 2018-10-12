package org.icij.extract.cli;

import java.util.function.Function;

import org.icij.task.Option;
import org.icij.task.Options;

public class CommonsTransformer implements Function<Options<String>, org.apache.commons.cli.Options> {

	@Override
	public org.apache.commons.cli.Options apply(final Options<String> options) {
		final org.apache.commons.cli.Options commonsOptions = new org.apache.commons.cli.Options();

		for (Option<String> option : options) {
			String code = option.code() == null ? null : option.code().toString();

			// The DefaultParser in commons-cli clones option objects before updating the value.
			// Work around this by overriding the clone method.
			org.apache.commons.cli.Option commonsOption = new org.apache.commons.cli.Option(code, option.name(),
					true, option.description()) {

				private static final long serialVersionUID = 7104410298761951462L;

				@Override
				public org.apache.commons.cli.Option clone() {
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
