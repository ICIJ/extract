package org.icij.extract;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.PathMatcher;
import java.nio.file.FileSystems;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Scanner {
	private final Logger logger;

	private final Queue queue;
	private PathMatcher matcher;

	public Scanner(Logger logger, Queue queue) {
		this.logger = logger;
		this.queue = queue;
	}

	public void setGlob(String pattern) {
		matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
	}

	public void scan(Path path) throws IOException {
		if (Files.isRegularFile(path)) {
			queue.queue(path);
		} else {
			scanDirectory(path);
		}
	}

	private void scanDirectory(Path path) throws IOException {

		// TODO: By default, symlinks are not followed. Add support for allowing them to be followed by specifying `Set<FileVisitOption> options` as an argument.
		Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {

				// TODO: Support a pattern for excluding directories, returning SKIP_SUBTREE if matching.
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (null == matcher || matcher.matches(file.getFileName())) {
					queue.queue(file);
				}

				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {

				// Don't re-throw the error. Scanning must be robust. Just log it.
				logger.log(Level.SEVERE, "Unable to read file attributes.", e);
				return FileVisitResult.CONTINUE;
			}
		});
	}
}
