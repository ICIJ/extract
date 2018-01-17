package org.icij.extract.io.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

/**
 * Create a {@link PathMatcher} that matches hidden files by checking the DOS hidden file attribute.
 *
 * @since 1.0.0-beta
 */
public class DosHiddenFileMatcher implements PathMatcher {

	@Override
	public boolean matches(final Path path) {
		try {
			final Boolean hidden = (Boolean) Files.getAttribute(path, "dos:hidden");

			return hidden != null && Boolean.TRUE.equals(hidden); 
		} catch (IOException e) {
			return false;
		}
	}
}
