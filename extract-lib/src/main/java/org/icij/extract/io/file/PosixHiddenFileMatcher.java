package org.icij.extract.io.file;

import java.nio.file.Path;
import java.nio.file.PathMatcher;

/**
 * Create a {@link PathMatcher} that matches hidden files by checking if the last element in the path starts with a dot.
 *
 * @since 1.0.0-beta
 */
public class PosixHiddenFileMatcher implements PathMatcher {

	@Override
	public boolean matches(final Path path) {
		final Path fileName = path.getFileName();

		return null != fileName && fileName.toString().startsWith(".");
	}
}
