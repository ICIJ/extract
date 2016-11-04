package org.icij.extract.tasks.factories;

import org.icij.extract.core.PathQueue;
import org.icij.extract.core.Scanner;
import org.icij.task.DefaultOption;

/**
 * Factory methods for creating {@link Scanner} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0
 */
public class ScannerFactory {

	/**
	 *  Create a {@link Scanner} from the given commandline parameters.
	 *
	 * @param options the options to parse
	 * @param queue the queue that the scanner will put file paths on
	 * @return a new scanner
	 * @throws IllegalArgumentException when one of the commandline parameters is invalid
	 */
	public static Scanner createScanner(final DefaultOption.Set options, final PathQueue queue) throws
			IllegalArgumentException {
		final Scanner scanner = new Scanner(queue);

		scanner.ignoreSystemFiles(!options.get("include-os-files").on());
		scanner.ignoreHiddenFiles(!options.get("include-hidden-files").on());
		scanner.followSymLinks(options.get("follow-symlinks").on());

		final String[] includePatterns = options.get("include-pattern").values();
		final String[] excludePatterns = options.get("exclude-pattern").values();

		for (String includePattern : includePatterns) {
			scanner.include(includePattern);
		}

		for (String excludePattern : excludePatterns) {
			scanner.exclude(excludePattern);
		}

		return scanner;
	}
}
