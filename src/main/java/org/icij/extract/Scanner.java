package org.icij.extract;

import java.util.Queue;
import java.util.Set;
import java.util.EnumSet;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitOption;
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
	private final Consumer consumer;

	private PathMatcher includeMatcher;
	private PathMatcher excludeMatcher;

	private int maxDepth = Integer.MAX_VALUE;
	private boolean followLinks = false;

	private SimpleFileVisitor visitor = new SimpleFileVisitor<Path>() {
		@Override
		public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
			if (null != excludeMatcher && excludeMatcher.matches(directory)) {
				return FileVisitResult.SKIP_SUBTREE;
			}

			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			if (null != includeMatcher && !includeMatcher.matches(file)) {
				return FileVisitResult.CONTINUE;
			}

			if (null != excludeMatcher && excludeMatcher.matches(file)) {
				return FileVisitResult.CONTINUE;
			}

			scanFile(file);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {

			// Don't re-throw the error. Scanning must be robust. Just log it.
			logger.log(Level.SEVERE, "Unable to read file attributes.", e);
			return FileVisitResult.CONTINUE;
		}
	};

	public Scanner(Logger logger, Queue queue) {
		this.queue = queue;
		this.logger = logger;
		this.consumer = null;
	}

	public Scanner(Logger logger, Consumer consumer) {
		this.queue = null;
		this.logger = logger;
		this.consumer = consumer;
	}

	public void setIncludeGlob(String pattern) {
		includeMatcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
	}

	public void setExcludeGlob(String pattern) {
		excludeMatcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
	}

	public void followSymLinks() {
		followLinks = true;
	}

	public void scan(Path path) {
		if (Files.isRegularFile(path)) {
			scanFile(path);
		} else {
			scanDirectory(path);
		}
	}

	private void scanFile(Path file) {
		if (null != queue) {
			queue.add(file);
		} else {
			consumer.consume(file);
		}
	}

	private void scanDirectory(Path path) {
		Set<FileVisitOption> options = null;

		if (followLinks) {
			options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
		} else {
			options = EnumSet.noneOf(FileVisitOption.class);
		}

		try {
			Files.walkFileTree(path, options, maxDepth, visitor);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while scanning directory.", e);
		}
	}
}
