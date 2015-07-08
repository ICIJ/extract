package org.icij.extract.matcher;

import java.nio.file.PathMatcher;
import java.nio.file.FileSystem;

/**
 * Create a {@link PathMatcher} that matches hidden files according
 * to a file system type.
 *
 * @since 1.0.0-beta
 */
public abstract class HiddenFileMatcherFactory {

	public static PathMatcher createMatcher(final FileSystem fileSystem) {
		if (fileSystem.supportedFileAttributeViews().contains("dos")) {
			return new DosHiddenFileMatcher();
		} else {
			return new PosixHiddenFileMatcher();
		}
	}
}
