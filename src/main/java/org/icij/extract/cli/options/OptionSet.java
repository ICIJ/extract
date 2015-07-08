package org.icij.extract.cli.options;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class OptionSet {

	public final Option[] options;

	public OptionSet(Option... options) {
		this.options = options;
	}

	public void addToOptions(final Options options) {
		for (Option option : this.options) {
			options.addOption(option);
		}
	}
}
