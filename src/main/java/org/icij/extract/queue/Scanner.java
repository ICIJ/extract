package org.icij.extract.queue;

import org.icij.events.Notifiable;
import org.icij.executor.ExecutorProxy;
import org.icij.concurrent.*;
import org.icij.io.file.matcher.*;

import org.icij.task.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.ArrayDeque;

import java.io.IOException;

import java.util.concurrent.*;

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
public class Scanner extends ExecutorProxy {

	private static final Logger logger = LoggerFactory.getLogger(Scanner.class);

	protected final BlockingQueue<Path> queue;

	private final ArrayDeque<String> includeGlobs = new ArrayDeque<>();
	private final ArrayDeque<String> excludeGlobs = new ArrayDeque<>();
	private final SealableLatch latch;
	private final Notifiable notifiable;
	private long queued = 0;

	private int maxDepth = Integer.MAX_VALUE;
	private boolean followLinks = false;
	private boolean ignoreHiddenFiles = false;
	private boolean ignoreSystemFiles = true;

	/**
	 * @see Scanner(BlockingQueue, SealableLatch, Notifiable)
	 */
	public Scanner(final BlockingQueue<Path> queue) {
		this(queue, null, null);
	}

	/**
	 * @see Scanner(BlockingQueue, SealableLatch, Notifiable)
	 */
	public Scanner(final BlockingQueue<Path> queue, final SealableLatch latch) {
		this(queue, latch, null);
	}

	/**
	 * Creates a {@code Scanner} that sends all results straight to the underlying {@link BlockingQueue<Path>} on a single thread.
	 *
	 * @param queue results from the scanner will be put on this queue
	 * @param latch signalled when a path is queued
	 * @param notifiable receives notifications when new file paths are queued
	 */
	public Scanner(final BlockingQueue<Path> queue, final SealableLatch latch, final Notifiable notifiable) {
		super(Executors.newSingleThreadExecutor());
		this.queue = queue;
		this.notifiable = notifiable;
		this.latch = latch;
	}

	public Scanner(final BlockingQueue<Path> queue, final SealableLatch latch, final Notifiable notifiable, final
	Options<String> options) {
		this(queue, latch, notifiable);
		options.get("include-os-files").parse().asBoolean().ifPresent(this::ignoreSystemFiles);
		options.get("include-hidden-files").parse().asBoolean().ifPresent(this::ignoreHiddenFiles);
		options.get("follow-symlinks").parse().asBoolean().ifPresent(this::followSymLinks);

		final List<String> includePatterns = options.get("include-pattern").values();
		final List<String> excludePatterns = options.get("exclude-pattern").values();

		for (String includePattern : includePatterns) {
			include(includePattern);
		}

		for (String excludePattern : excludePatterns) {
			exclude(excludePattern);
		}
	}

	/**
	 * Add a glob pattern for including files. Files not matching the pattern will be ignored.
	 *
	 * @param pattern the glob pattern
	 */
	public void include(final String pattern) {
		includeGlobs.add("glob:" + pattern);
	}

	/**
	 * Add a glob pattern for excluding files and directories.
	 *
	 * @param pattern the glob pattern
	 */
	public void exclude(final String pattern) {
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
	 * File names starting with a dot will always be ignored if set to {@literal true}, but DOS hidden files will
	 * only be ignored if the filesystem supports the DOS hidden fileattribute.
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
	 * Get the latch.
	 *
	 * @return The latch or null if none is set.
	 */
	public SealableLatch getLatch() {
		return latch;
	}

	/**
	 * @return The total number of queued paths over the lifetime of this scanner.
	 */
	public long queued() {
		return queued;
	}

	/**
	 * Queue a scanning job.
	 *
	 * Jobs are put in an unbounded queue and executed in serial, in a separate thread.
	 * This method doesn't block. Call {@link #awaitTermination(long, TimeUnit)} to block.
	 *
	 * @param base the base path, stripped from file paths encountered by the scanner before queuing
	 * @param path the path to scan
	 * @return A {@link Future} that can be used to wait on the result or cancel.
	 */
	public Future<Path> scan(final Path base, final Path path) {
		final FileSystem fileSystem = path.getFileSystem();
		final ScannerVisitor visitor = new ScannerVisitor(base, path);

		// In order to make hidden-file-ignoring logic more predictable, always ignore file names starting with a
		// dot, but only ignore DOS hidden files if the file system supports that attribute.
		if (ignoreHiddenFiles) {
			visitor.exclude(new PosixHiddenFileMatcher());
			if (fileSystem.supportedFileAttributeViews().contains("dos")) {
				visitor.exclude(new DosHiddenFileMatcher());
			}
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
	 * Submit all of the given paths to the scanner for execution, returning a list of {@link Future} objects
	 * representing those tasks.
	 *
	 * @see #scan(Path, Path)
	 * @return a {@link Future} for each path scanned
	 */
	public List<Future<Path>> scan(final Path base, final Path[] paths) {
		final List<Future<Path>> futures = new ArrayList<>();

		for (Path path : paths) futures.add(scan(base, path));
		return futures;
	}

	/**
	 * @see #scan(Path, Path[])
	 */
	public List<Future<Path>> scan(final String base, final String[] paths) {
		final Path[] _paths = new Path[paths.length];

		for (int i = 0; i < paths.length; i++) _paths[i] = Paths.get(paths[i]);

		if (null != base) {
			return scan(Paths.get(base), _paths);
		}

		return scan(_paths);
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
	 * @see #scan(String, String[])
	 */
	public List<Future<Path>> scan(final Path[] paths) { return scan(null, paths); }

	/**
	 * @see #scan(Path, Path[])
	 */
	public List<Future<Path>> scan(final String[] paths) { return scan(null, paths); }

	private class ScannerVisitor extends SimpleFileVisitor<Path> implements Callable<Path> {

		private final ArrayDeque<PathMatcher> includeMatchers = new ArrayDeque<>();
		private final ArrayDeque<PathMatcher> excludeMatchers = new ArrayDeque<>();

		private final Path base;
		private final Path path;

		/**
		 * Instantiate a new task for scanning the given path.
		 *
		 * @param base the base path, to be stripped from scanned paths before queuing
		 * @param path the path to scan
		 */
		ScannerVisitor(final Path base, final Path path) {
			this.base = base;
			this.path = path;
		}

		/**
		 * Recursively walks the file tree of a directory. When walking is finished or stopped by an exception, the
		 * latch is sealed and signalled.
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
				logger.error(String.format("Error while scanning path: \"%s\".", path), e);
				throw e;
			} finally {
				if (null != latch){
					latch.seal();
					latch.signal();
				}
			}

			logger.info(String.format("Completed scan of: \"%s\".", path));
			return path;
		}

		/**
		 * Queue a result from the scanner. Blocks until a queue slot is available.
		 *
		 * @throws InterruptedException if interrupted while waiting for a queue slot
		 */
		void queue(final Path file) throws InterruptedException {
			if (null != base && file.startsWith(base)) {
				queue.put(file.subpath(base.getNameCount(), file.getNameCount()));
			} else {
				queue.put(file);
			}

			queued++;

			if (null != latch) {
				latch.signal();
			}

			if (null != notifiable) {
				notifiable.notifyListeners(file);
			}
		}

		/**
		 * Add a path matcher for files to exclude.
		 *
		 * @param matcher the matcher
		 */
		void exclude(final PathMatcher matcher) {
			excludeMatchers.add(matcher);
		}

		/**
		 * Add a path matcher for files to include.
		 *
		 * @param matcher the matcher
		 */
		void include(final PathMatcher matcher) {
			includeMatchers.add(matcher);
		}

		/**
		 * Check whether a path should be excluded.
		 *
		 * @param path path to check
		 * @return whether the path should be excluded
		 */
		boolean shouldExclude(final Path path) {
			return matches(path, excludeMatchers);
		}

		/**
		 * Check whether a path should be included.
		 *
		 * @param path path to check
		 * @return whether the path should be included
		 */
		boolean shouldInclude(final Path path) {
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
		public FileVisitResult preVisitDirectory(final Path directory, final BasicFileAttributes attributes) throws
				IOException {
			if (Thread.currentThread().isInterrupted()) {
				logger.warn("Scanner interrupted. Terminating job.");
				return FileVisitResult.TERMINATE;
			}

			if (shouldExclude(directory)) {
				return FileVisitResult.SKIP_SUBTREE;
			}

			logger.info(String.format("Entering directory: \"%s\".", directory));
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes) throws IOException {
			if (Thread.currentThread().isInterrupted()) {
				logger.warn("Scanner interrupted. Terminating job.");
				return FileVisitResult.TERMINATE;
			}

			// From the documentation:
			// "When following links, and the attributes of the target cannot be read, then this method attempts to
			// get the BasicFileAttributes of the link."
			if (attributes.isSymbolicLink()) {
				if (followLinks) {
					logger.warn(String.format("Unable to read attributes of symlink target: \"%s\". Skipping.", file));
				}

				return FileVisitResult.CONTINUE;
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
				logger.warn("Interrupted. Terminating scanner.");
				return FileVisitResult.TERMINATE;
			} catch (Exception e) {
				logger.error(String.format("Exception while queuing file: \"%s\".", file), e);
				throw e;
			}

			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {

			// If the file or directory was going to be excluded anyway, suppress the exception.
			// Don't re-throw the error. Scanning must be robust. Just log it.
			if (!shouldExclude(file)) {
				logger.error(String.format("Unable to read attributes of file: \"%s\".", file), e);
			}

			return FileVisitResult.CONTINUE;
		}
	}
}
