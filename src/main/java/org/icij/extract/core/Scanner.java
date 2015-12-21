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
import java.util.concurrent.atomic.AtomicBoolean;

import java.nio.file.Path;
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
 * Each time {@link #scan} is called, the job is put in an unbounded queue
 * and executed in serial. This makes sense as it's usually the file system
 * which is a bottleneck and not the CPU, so parallelization won't help.
 *
 * The {@link #scan} method is non-blocking, which is useful for creating
 * parallelized producer-consumer setups, where files are processed as
 * they're scanned.
 *
 * Encountered file paths are put in a given queue. This is a classic producer,
 * putting elements in a queue which are then extracted by a consumer.
 *
 * The queue should be bounded, to avoid the scanner filling up memory, but the
 * bound should be high enough to create a significant buffer between the scanner
 * and the consumer.
 *
 * Paths are pushed into the queue synchronously and if the queue is bounded, only
 * when a space becomes available.
 *
 * This implementation is thread-safe.
 *
 * Scanning aims to be robust. All exceptions thrown by the handler, except
 * {@link InterruptedException}, are swallowed.
 *
 * @since 1.0.0-beta
 */
public class Scanner {
	protected final Logger logger;
	protected final Queue queue;
	protected final ExecutorService executor = Executors.newSingleThreadExecutor();

	protected final ArrayDeque<String> includeGlobs = new ArrayDeque<>();
	protected final ArrayDeque<String> excludeGlobs = new ArrayDeque<>();

	private final AtomicBoolean stopped = new AtomicBoolean();

	private int maxDepth = Integer.MAX_VALUE;
	private boolean followLinks = false;
	private boolean ignoreHiddenFiles = false;
	private boolean ignoreOSFiles = true;

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

	public void setMaxDepth(final int maxDepth) {
		this.maxDepth = maxDepth;
	}

	public int getMaxDepth() {
		return maxDepth;
	}

	/**
	 * Queue a scanning job.
	 *
	 * Jobs are put in an unbounded queue and executed in serial, in a separate thread.
	 * This method doesn't block. Call {@link #finish} to block.
	 *
	 * @return A {@link Future} that can be used to wait on the result or cancel.
	 */
	public Future<Path> scan(final Path path) {
		logger.info(String.format("Queuing scan of: %s.", path));
		return executor.submit(new ScannerTask(path));
	}

	/**
	 * Shut down the executor and await termination of all running and waiting jobs.
	 *
	 * This method blocks until all paths have been scanned.
	 *
	 * This method should be called to free up resources when the scanner
	 * is no longer needed.
	 *
	 * @throws InterruptedException if the thread is interrupted while waiting.
	 */
	public void finish() throws InterruptedException {
		logger.info("Shutting down scanner executor.");
		executor.shutdown();

		do {
			logger.info("Awaiting completion of scanner.");
		} while (!executor.awaitTermination(60, TimeUnit.SECONDS));
		logger.info("Scanner finished.");
	}

	/**
	 * Stop scanning.
	 *
	 * @return Whether the scanner was stopped or already stopped.
	 */
	public boolean stop() {
		logger.info("Stopping scanning.");
		return stopped.compareAndSet(false, true);
	}

	/**
	 * Queue a result from the scanner. Blocks until a queue slot is available.
	 *
	 * @throws InterruptedException if interrupted while waiting for a queue slot
	 */
	protected void queue(final Path file) throws InterruptedException {
		queue.put(file);
	}

	/**
	 * Recursively walks the file tree of a directory.
	 *
	 * @param directory the directory at which to start walking
	 */
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
			logger.info(String.format("Starting scan of: %s.", path));
			if (stopped.get()) {
				return path;
			}

			try {
				if (Files.isRegularFile(path)) {
					queue(path);
				} else {
					scanDirectory(path);
				}
			} catch (IOException e) {
				logger.log(Level.SEVERE, String.format("Error while scanning directory: %s.",
					path), e);
				throw e;
			}

			return path;
		}
	}

	protected class Visitor extends SimpleFileVisitor<Path> {

		protected final ArrayDeque<PathMatcher> includeMatchers = new ArrayDeque<>();
		protected final ArrayDeque<PathMatcher> excludeMatchers = new ArrayDeque<>();

		private int visited = 0;

		@Override
		public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
			if (stopped.get() || Thread.currentThread().isInterrupted()) {
				logger.warning("Scanner stopped. Terminating job.");
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
			if (stopped.get() || Thread.currentThread().isInterrupted()) {
				logger.warning("Scanner stopped. Terminating job.");
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

			// Handle errors robustly.
			// This means that exceptions thrown by the handler are logged and
			// swallowed. Except InterruptedExceptions, which are an instruction
			// to kill the thread.
			try {
				queue(file);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
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

			// If the file or directory was going to be excluded anyway, suppress
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
