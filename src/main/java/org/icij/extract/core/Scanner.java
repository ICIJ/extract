package org.icij.extract.core;

import org.icij.extract.matcher.*;

import java.util.List;
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
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.PathMatcher;
import java.nio.file.FileSystem;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Scanner for scanning the directory tree starting at a given path.
 *
 * Each time {@link #scan} is called, the job is put in an unbounded queue and executed in serial. This makes sense as
 * it's usually the file system which is a bottleneck and not the CPU, so parallelization won't help.
 *
 * The {@link #scan} method is non-blocking, which is useful for creating parallelized producer-consumer setups, where
 * files are processed as they're scanned.
 *
 * Encountered file paths are put in a given queue. This is a classic producer, putting elements in a queue which are
 * then extracted by a consumer.
 *
 * The queue should be bounded, to avoid the scanner filling up memory, but the bound should be high enough to create a
 * significant buffer between the scanner and the consumer.
 *
 * Paths are pushed into the queue synchronously and if the queue is bounded, only when a space becomes available.
 *
 * This implementation is thread-safe.
 *
 * @since 1.0.0-beta
 */
public class Scanner {
	protected final Logger logger;
	protected final Queue queue;
	protected final ExecutorService executor = Executors.newSingleThreadExecutor();

	protected final ArrayDeque<String> includeGlobs = new ArrayDeque<>();
	protected final ArrayDeque<String> excludeGlobs = new ArrayDeque<>();

	private int maxDepth = Integer.MAX_VALUE;
	private boolean followLinks = false;
	private boolean ignoreHiddenFiles = false;
	private boolean ignoreSystemFiles = true;

	/**
	 * Creates a {@code Scanner} that sends all results straight to
	 * the underlying {@link Queue}.
	 *
	 * @param logger logger
	 * @param queue results from the scanner will be put on this queue
	 */
	public Scanner(final Logger logger, final Queue queue) {
		this.logger = logger;
		this.queue = queue;
	}

	/**
	 * Add a glob pattern for including files and directories. Files and directories not matching the pattern will be
	 * ignored.
	 *
	 * @param pattern the glob pattern
	 */
	public void addIncludeGlob(final String pattern) {
		includeGlobs.add("glob:" + pattern);
	}

	/**
	 * Add a glob pattern for excluding files and directories.
	 *
	 * @param pattern the glob pattern
	 */
	public void addExcludeGlob(final String pattern) {
		excludeGlobs.add("glob:" + pattern);
	}

	/**
	 * Set whether symlinks should be followed.
	 *
	 * @param followLinks whether to follow symlinks
	 */
	public void followSymLinks(final boolean followLinks) {
		this.followLinks = followLinks;
	}

	/**
	 * Check whether symlinks will be followed.
	 *
	 * @return whether symlinks will be followed
	 */
	public boolean followSymLinks() {
		return followLinks;
	}

	/**
	 * Set whether hidden files should be ignored.
	 *
	 * @param ignoreHiddenFiles whether to ignore hidden files
	 */
	public void ignoreHiddenFiles(final boolean ignoreHiddenFiles) {
		this.ignoreHiddenFiles = ignoreHiddenFiles;
	}

	/**
	 * Check whether hidden files will be ignored.
	 *
	 * @return whether hidden files will be ignored
	 */
	public boolean ignoreHiddenFiles() {
		return ignoreHiddenFiles;
	}

	/**
	 * Set whether system files should be ignored.
	 *
	 * @param ignoreSystemFiles whether to ignore system files
	 */
	public void ignoreSystemFiles(final boolean ignoreSystemFiles) {
		this.ignoreSystemFiles = ignoreSystemFiles;
	}

	/**
	 * Check whether system files will be ignored.
	 *
	 * @return whether system files are ignore
	 */
	public boolean ignoreSystemFiles() {
		return ignoreSystemFiles;
	}

	/**
	 * Set the maximum depth to recurse when scanning.
	 *
	 * @param maxDepth maximum depth
	 */
	public void setMaxDepth(final int maxDepth) {
		this.maxDepth = maxDepth;
	}

	/**
	 * Get the currently set maximum depth to recurse when scanning.
	 *
	 * @return maximum depth
	 */
	public int getMaxDepth() {
		return maxDepth;
	}

	/**
	 * Queue a scanning job.
	 *
	 * Jobs are put in an unbounded queue and executed in serial, in a separate thread.
	 * This method doesn't block. Call {@link #awaitTermination()} to block.
	 *
	 * @param base the base path, stripped from paths before queuing
	 * @param path the path to scan
	 * @return A {@link Future} that can be used to wait on the result or cancel.
	 */
	public Future<Path> scan(final Path base, final Path path) {
		final FileSystem fileSystem = path.getFileSystem();
		final ScannerVisitor visitor = new ScannerVisitor(base, path);

		if (ignoreHiddenFiles) {
			visitor.exclude(HiddenFileMatcherFactory
					.createMatcher(fileSystem));
		}

		if (ignoreSystemFiles) {
			visitor.exclude(new SystemFileMatcher());
		}

		for (String excludeGlob : excludeGlobs) {
			visitor.exclude(fileSystem.getPathMatcher(excludeGlob));
		}

		for (String includeGlob : includeGlobs) {
			visitor.include(fileSystem.getPathMatcher(includeGlob));
		}

		logger.info(String.format("Queuing scan of: \"%s\".", path));
		return executor.submit(visitor);
	}

	/**
	 * @see Scanner#scan(Path, Path)
	 */
	public Future<Path> scan(final String base, final String path) {
		if (null != base) {
			return scan(Paths.get(base), Paths.get(path));
		} else {
			return scan(path);
		}
	}

	/**
	 * @see Scanner#scan(Path, Path)
	 */
	public Future<Path> scan(final Path path) {
		return scan(null, path);
	}

	/**
	 * @see Scanner#scan(Path, Path)
	 */
	public Future<Path> scan(final String path) {
		return scan(null, Paths.get(path));
	}

	/**
	 * Await termination of all running and waiting jobs.
	 *
	 * This method blocks until all paths have been scanned.
	 *
	 * @throws InterruptedException if the thread is interrupted while waiting.
	 */
	public void awaitTermination() throws InterruptedException {
		do {
			logger.info("Awaiting completion of scanner.");
		} while (!executor.awaitTermination(60, TimeUnit.SECONDS));
		logger.info("Scanner finished.");
	}

	/**
	 * Shut down the scanner, waiting for all previously queued tasks to complete.
	 *
	 * This method should be called to free up resources when the scanner is no longer needed.
	 */
	public void shutdown() {
		logger.info("Shutting down scanner.");
		executor.shutdown();
	}

	/**
	 * Shut down the scanner, interrupting running tasks and cancelling waiting ones.
	 *
	 * @return list of tasks that never commenced execution
	 */
	public List<Runnable> shutdownNow() {
		logger.info("Shutting down scanner immediately.");
		return executor.shutdownNow();
	}

	protected class ScannerVisitor extends SimpleFileVisitor<Path> implements Callable<Path> {

		private final ArrayDeque<PathMatcher> includeMatchers = new ArrayDeque<>();
		private final ArrayDeque<PathMatcher> excludeMatchers = new ArrayDeque<>();

		private int visited = 0;

		protected final Path base;
		protected final Path path;

		/**
		 * Instantiate a new task for scanning the given path.
		 *
		 * @param base the base path, to be stripped from scanned paths before queuing
		 * @param path the path to scan
		 */
		public ScannerVisitor(final Path base, final Path path) {
			this.base = base;
			this.path = path;
		}

		/**
		 * Recursively walks the file tree of a directory.
		 *
		 * @return the path at which scanning started
		 */
		@Override
		public Path call() throws Exception {
			final Set<FileVisitOption> options;

			if (followLinks) {
				options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
			} else {
				options = EnumSet.noneOf(FileVisitOption.class);
			}

			logger.info(String.format("Starting scan of: \"%s\".", path));
			try {
				Files.walkFileTree(path, options, maxDepth, this);
			} catch (IOException e) {
				logger.log(Level.SEVERE, String.format("Error while scanning path: \"%s\".",
						path), e);
				throw e;
			}

			logger.info(String.format("Completed scan of: \"%s\".", path));
			return path;
		}

		/**
		 * Queue a result from the scanner. Blocks until a queue slot is available.
		 *
		 * @throws InterruptedException if interrupted while waiting for a queue slot
		 */
		protected void queue(final Path file) throws InterruptedException {
			if (null != base && file.startsWith(base)) {
				queue.put(file.subpath(base.getNameCount(), file.getNameCount()));
			} else {
				queue.put(file);
			}
		}

		/**
		 * Add a path matcher for files to exclude.
		 *
		 * @param matcher the matcher
		 */
		public void exclude(final PathMatcher matcher) {
			excludeMatchers.add(matcher);
		}

		/**
		 * Add a path matcher for files to include.
		 *
		 * @param matcher the matcher
		 */
		public void include(final PathMatcher matcher) {
			includeMatchers.add(matcher);
		}

		/**
		 * Check whether a path should be excluded.
		 *
		 * @param path path to check
		 * @return whether the path should be excluded
		 */
		public boolean shouldExclude(final Path path) {
			return matches(path, excludeMatchers);
		}

		/**
		 * Check whether a path should be included.
		 *
		 * @param path path to check
		 * @return whether the path should be included
		 */
		public boolean shouldInclude(final Path path) {
			return includeMatchers.size() == 0 || matches(path, includeMatchers);
		}

		/**
		 * Check whether a path matches any of the given matchers.
		 *
		 * @param path path to check
		 * @return whether the path matches
		 */
		private boolean matches(final Path path, final ArrayDeque<PathMatcher> matchers) {
			for (PathMatcher matcher : matchers) {
				if (matcher.matches(path)) {
					return true;
				}
			}

			return false;
		}

		@Override
		public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
			if (Thread.currentThread().isInterrupted()) {
				logger.warning("Scanner interrupted. Terminating job.");
				return FileVisitResult.TERMINATE;
			}

			if (shouldExclude(directory)) {
				return FileVisitResult.SKIP_SUBTREE;
			}

			logger.info(String.format("Entering directory: \"%s\".", directory));
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			if (Thread.currentThread().isInterrupted()) {
				logger.warning("Scanner interrupted. Terminating job.");
				return FileVisitResult.TERMINATE;
			}

			visited++;
			if (visited % 10000 == 0) {
				logger.info(String.format("Scanner visited %d files.", visited));
			}

			// Only skip the file if all of the include matchers return false.
			if (!shouldInclude(file)) {
				return FileVisitResult.CONTINUE;
			}

			if (shouldExclude(file)) {
				return FileVisitResult.CONTINUE;
			}

			try {
				queue(file);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warning("Interrupted. Terminating scanner.");
				return FileVisitResult.TERMINATE;
			} catch (Exception e) {
				logger.log(Level.SEVERE, String.format("Exception while queuing file: \"%s\"", file), e);
				throw e;
			}

			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {

			// If the file or directory was going to be excluded anyway, suppress
			// the exception.
			// Don't re-throw the error. Scanning must be robust. Just log it.
			if (!shouldExclude(file)) {
				logger.log(Level.SEVERE, "Unable to read attributes of file: \"%s\".", e);
			}

			return FileVisitResult.CONTINUE;
		}
	}
}
