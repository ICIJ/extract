package org.icij.extract.tasks.factories;

import org.icij.events.Notifiable;
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
	public static Scanner createScanner(final DefaultOption.Set options, final PathQueue queue, final Notifiable
			notifiable) throws IllegalArgumentException {
		final Scanner scanner = new Scanner(queue, notifiable);

		options.get("include-os-files").toggle().ifPresent(scanner::ignoreSystemFiles);
		options.get("include-hidden-files").toggle().ifPresent(scanner::ignoreHiddenFiles);
		options.get("follow-symlinks").toggle().ifPresent(scanner::followSymLinks);

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
