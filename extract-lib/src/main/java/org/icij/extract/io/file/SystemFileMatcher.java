package org.icij.extract.io.file;

import java.nio.file.Path;
import java.nio.file.PathMatcher;

/**
 * Create a {@link PathMatcher} that matches operating-system-generated files.
 *
 * @since 1.0.0-beta
 */
public class SystemFileMatcher implements PathMatcher {

	@Override
	public boolean matches(final Path path) {
		final Path fileName = path.getFileName();

		if (null == fileName) {
			return false;
		}

		final String name = fileName.toString();

		switch (name) {
		case ".DS_Store":
		case ".AppleDouble":
		case ".Spotlight-V100":
		case ".Trashes":
		case "._.lost+found":
		case ".fseventsd":
		case "lost+found":
		case "Thumbs.db":
		case "$RECYCLE.BIN":
			return true;
		}

		// Ignore AppleDouble dot files.
		return name.startsWith("._");
	}
}
