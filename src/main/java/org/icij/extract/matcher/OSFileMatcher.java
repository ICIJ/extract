package org.icij.extract.matcher;

import java.nio.file.Path;
import java.nio.file.PathMatcher;

/**
 * Create a {@link PathMatcher} that matches operating-system-generated
 * files.
 *
 * @since 1.0.0-beta
 */
public class OSFileMatcher implements PathMatcher {

	public boolean matches(final Path path) {
		final String name = path.getFileName().toString();

		switch (name) {
		case ".DS_Store":
		case ".AppleDouble":
		case ".Spotlight-V100":
		case ".Trashes":
		case "._.Trashes":
		case ".fseventsd":
		case "lost+found":
		case "Thumbs.db":
		case "$RECYCLE.BIN":
			return true;
		}

		// Ignore AppleDouble dot files.
		if (name.startsWith("._")) {
			return true;
		}

		return false;
	}
}
