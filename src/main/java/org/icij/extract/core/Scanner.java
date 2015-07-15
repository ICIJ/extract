package org.icij.extract.core;

import org.icij.extract.matcher.*;

import java.util.Set;
import java.util.EnumSet;
import java.util.ArrayDeque;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

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
 * Each time {@link #scan} is called, the job is put in an unbounded queue
 * and executed in serial. This makes sense as it's usually the file system
 * which is a bottleneck and not the CPU, so parallelization won't help.
 *
 * The {@link #scan} method is non-blocking, which is useful for creating
 * parallelized producer-consumer setups, where files are processed as
 * they're scanned.
 *
 * @since 1.0.0-beta
 */
public abstract class Scanner {
	protected final Logger logger;
	protected final ExecutorService executor = Executors.newSingleThreadExecutor();

	protected ArrayDeque<String> includeGlobs = new ArrayDeque<String>();
	protected ArrayDeque<String> excludeGlobs = new ArrayDeque<String>();

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

	/**
	 * Queue a scanning job.
	 *
	 * Jobs are put in an unbounded and executed in serial, in a separate thread.
	 * This method doesn't block. Call {@link #awaitTermination} to block.
	 *
	 * @return A {@code Future<Path>} for the scanning job.
	 */
	public Future<Path> scan(final Path path) {
		logger.info("Queuing scan of \"" + path + "\".");
		return executor.submit(new ScannerTask(path));
	}

	/**
	 * Shut down the executor.
	 *
	 * This method should be called to free up resources when the scanner
	 * is no longer needed.
	 */
	public void shutdown() {
		logger.info("Shutting down scanner executor.");
		executor.shutdown();
	}

	/**
	 * Shut down the executor immediately.
	 *
	 * This method should be called to interrupt scanning.
	 */
	public void shutdownNow() {
		logger.info("Forcibly shutting down scanner executor.");
		executor.shutdownNow();
	}

	/**
	 * Await termination of all running and waiting jobs.
	 *
	 * This method blocks until all paths have been scanned.
	 *
	 * @throws InterruptedException if the thread is interrupted while waiting.
	 */
	public void awaitTermination() throws InterruptedException {
		while (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
			logger.info("Awaiting completion of scanner.");
		}
	}

	protected abstract void handle(final Path file) throws Exception;

	protected void scanDirectory(final Path directory) throws IOException {
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

		Files.walkFileTree(directory, options, maxDepth, visitor);
	}

	protected class ScannerTask implements Callable<Path> {

		protected final Path path;

		public ScannerTask(final Path path) {
			this.path = path;
		}

		@Override
		public Path call() throws Exception {
			try {
				if (Files.isRegularFile(path)) {
					handle(path);
				} else {
					scanDirectory(path);
				}
			} catch (IOException e) {
				logger.log(Level.SEVERE, String.format("Error while scanning directory: %s.",
					path), e);
				throw e;

			// Only this thread was interrupted.
			// No need to stop the whole scanner.
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

			return path;
		}
	}

	protected class Visitor extends SimpleFileVisitor<Path> {

		protected ArrayDeque<PathMatcher> includeMatchers = new ArrayDeque<PathMatcher>();
		protected ArrayDeque<PathMatcher> excludeMatchers = new ArrayDeque<PathMatcher>();

		private int visited = 0;

		@Override
		public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
			if (Thread.currentThread().isInterrupted()) {
				logger.warning("Interrupted. Terminating scanner.");
				return FileVisitResult.TERMINATE;
			}

			for (PathMatcher excludeMatcher : excludeMatchers) {
				if (excludeMatcher.matches(directory)) {
					return FileVisitResult.SKIP_SUBTREE;
				}
			}

			logger.info("Entering directory: " + directory);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			if (Thread.currentThread().isInterrupted()) {
				logger.warning("Interrupted. Terminating scanner.");
				return FileVisitResult.TERMINATE;
			}

			boolean matched = true;
			visited++;

			if (visited % 10000 == 0) {
				logger.info(String.format("Scanner visited %d files.", visited));
			}

			// Only skip the file if all of the include matchers return false.
			for (PathMatcher includeMatcher : includeMatchers) {
				if (matched = includeMatcher.matches(file)) {
					break;
				}
			}

			if (!matched) {
				return FileVisitResult.CONTINUE;
			}

			for (PathMatcher excludeMatcher : excludeMatchers) {
				if (excludeMatcher.matches(file)) {
					return FileVisitResult.CONTINUE;
				}
			}

			try {
				handle(file);
			} catch (InterruptedException e) {
				logger.warning("Interrupted. Terminating scanner.");
				return FileVisitResult.TERMINATE;
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Exception while handling file.", e);
			}

			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
			boolean excluded = false;

			// If the file or directory was going to be excluded anyway, supress
			// the exception.
			for (PathMatcher excludeMatcher : excludeMatchers) {
				if (excluded = excludeMatcher.matches(file)) {
					break;
				}
			}

			// Don't re-throw the error. Scanning must be robust. Just log it.
			if (!excluded) {
				logger.log(Level.SEVERE, "Unable to read file attributes.", e);
			}

			return FileVisitResult.CONTINUE;
		}
	}
}
