package org.icij.extract.core;

import org.icij.extract.matcher.*;

import java.util.Set;
import java.util.EnumSet;
import java.util.ArrayDeque;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.PathMatcher;
import java.nio.file.FileSystem;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Base scanner for scanning the directory tree starting at a given path.
 *
 * Implementations must implement the {@link #handle(Path) handle} method.
 *
 * @since 1.0.0-beta
 */
public abstract class Scanner {
	protected final Logger logger;
	protected final CompletionService<Path> service = new ExecutorCompletionService<Path>(Executors.newSingleThreadExecutor());

	protected ArrayDeque<String> includeGlobs = new ArrayDeque<String>();
	protected ArrayDeque<String> excludeGlobs = new ArrayDeque<String>();

	private int waiting = 0;

	private int maxDepth = Integer.MAX_VALUE;
	private boolean followLinks = false;
	private boolean ignoreHiddenFiles = false;
	private boolean ignoreOSFiles = true;

	public Scanner(Logger logger) {
		this.logger = logger;
	}

	public void addIncludeGlob(final String pattern) {
		includeGlobs.add("glob:" + pattern);
	}

	public void addExcludeGlob(final String pattern) {
		excludeGlobs.add("glob:" + pattern);
	}

	public void followSymLinks(final boolean followLinks) {
		this.followLinks = followLinks;
	}

	public boolean followSymLinks() {
		return followLinks;
	}

	public void ignoreHiddenFiles(final boolean ignoreHiddenFiles) {
		this.ignoreHiddenFiles = ignoreHiddenFiles;
	}

	public boolean ignoreHiddenFiles() {
		return ignoreHiddenFiles;
	}

	public void ignoreOSFiles(final boolean ignoreOSFiles) {
		this.ignoreOSFiles = ignoreOSFiles;
	}

	public boolean ignoreOSFiles() {
		return ignoreOSFiles;
	}

	public void scan(Path path) {
		logger.info("Queuing scan of \"" + path + "\".");
		waiting++;
		service.submit(new ScannerTask(path), path);
	}

	public void awaitTermination() throws CancellationException, InterruptedException, ExecutionException {
		while (waiting > 0) {
			logger.info("Completed scan of \"" + service.take().get() + "\".");
			waiting--;
		}
	}

	protected abstract void handle(Path file);

	protected void scanDirectory(Path directory) {
		final Set<FileVisitOption> options;

		if (followLinks) {
			options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
		} else {
			options = EnumSet.noneOf(FileVisitOption.class);
		}

		final FileSystem fileSystem = directory.getFileSystem();
		final Visitor visitor = new Visitor();

		if (ignoreHiddenFiles) {
			visitor.excludeMatchers.add(HiddenFileMatcherFactory
				.createMatcher(fileSystem));
		}

		if (ignoreOSFiles) {
			visitor.excludeMatchers.add(new OSFileMatcher());
		}

		for (String excludeGlob : excludeGlobs) {
			visitor.excludeMatchers.add(fileSystem.getPathMatcher(excludeGlob));
		}

		for (String includeGlob : includeGlobs) {
			visitor.includeMatchers.add(fileSystem.getPathMatcher(includeGlob));
		}

		try {
			Files.walkFileTree(directory, options, maxDepth, visitor);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while scanning directory.", e);
		}
	}

	protected class ScannerTask implements Runnable {

		protected final Path path;

		public ScannerTask(Path path) {
			this.path = path;
		}

		public void run() {
			if (Files.isRegularFile(path)) {
				handle(path);
			} else {
				scanDirectory(path);
			}
		}
	}

	protected class Visitor extends SimpleFileVisitor<Path> {

		protected ArrayDeque<PathMatcher> includeMatchers = new ArrayDeque<PathMatcher>();
		protected ArrayDeque<PathMatcher> excludeMatchers = new ArrayDeque<PathMatcher>();

		public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
			for (PathMatcher excludeMatcher : excludeMatchers) {
				if (excludeMatcher.matches(directory)) {
					return FileVisitResult.SKIP_SUBTREE;
				}
			}

			logger.info("Entering directory: " + directory);
			return FileVisitResult.CONTINUE;
		}

		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			for (PathMatcher includeMatcher : includeMatchers) {
				if (!includeMatcher.matches(file)) {
					return FileVisitResult.CONTINUE;
				}
			}

			for (PathMatcher excludeMatcher : excludeMatchers) {
				if (excludeMatcher.matches(file)) {
					return FileVisitResult.CONTINUE;
				}
			}

			handle(file);
			return FileVisitResult.CONTINUE;
		}

		public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {

			// Don't re-throw the error. Scanning must be robust. Just log it.
			logger.log(Level.SEVERE, "Unable to read file attributes.", e);
			return FileVisitResult.CONTINUE;
		}
	}
}
