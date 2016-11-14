package org.icij.extract.tasks.factories;

import org.icij.concurrent.SealableLatch;
import org.icij.events.Notifiable;
import org.icij.extract.core.PathQueue;
import org.icij.extract.core.Scanner;
import org.icij.task.DefaultOption;

/**
 * Factory for creating {@link Scanner} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0
 */
public class ScannerFactory {

	private PathQueue queue = null;
	private Notifiable notifiable = null;
	private SealableLatch latch = null;
	private DefaultOption.Set options = null;

	public ScannerFactory withQueue(final PathQueue queue) {
		this.queue = queue;
		return this;
	}

	public ScannerFactory withNotifiable(final Notifiable notifiable) {
		this.notifiable = notifiable;
		return this;
	}

	public ScannerFactory withOptions(final DefaultOption.Set options) {
		this.options = options;
		return this;
	}

	public ScannerFactory withLatch(final SealableLatch latch) {
		this.latch = latch;
		return this;
	}

	public Scanner create() {
		final Scanner scanner = new Scanner(queue, latch, notifiable);

		options.get("include-os-files").asBoolean().ifPresent(scanner::ignoreSystemFiles);
		options.get("include-hidden-files").asBoolean().ifPresent(scanner::ignoreHiddenFiles);
		options.get("follow-symlinks").asBoolean().ifPresent(scanner::followSymLinks);

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
