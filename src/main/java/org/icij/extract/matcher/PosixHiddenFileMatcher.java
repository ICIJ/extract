package org.icij.extract.matcher;

import java.nio.file.Path;
import java.nio.file.PathMatcher;

/**
 * Create a {@link PathMatcher} that matches hidden files by
 * checking if the last element in the path starts with a dot.
 *
 * @since 1.0.0-beta
 */
public class PosixHiddenFileMatcher implements PathMatcher {

	public boolean matches(final Path path) {
		return path.getFileName().toString().startsWith(".");
	}
}
