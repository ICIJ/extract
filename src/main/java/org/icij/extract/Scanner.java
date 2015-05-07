package org.icij.extract;

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
	private Queue queue;
	private PathMatcher matcher;

	public Scanner(Queue queue) {
		this.queue = queue;
	}

	public void setGlob(String pattern) {
		matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
	}

	public void scan(Path directory) throws IOException {
		Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (null == matcher || matcher.matches(file.getFileName())) {
					queue.queue(file);
				}

				return FileVisitResult.CONTINUE;
			}
		});
	}
}
