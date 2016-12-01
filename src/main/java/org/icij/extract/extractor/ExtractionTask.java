package org.icij.extract.core;

import java.util.concurrent.Callable;

import java.nio.file.Path;

import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

import org.icij.extract.parser.ExcludedMediaTypeException;
import org.icij.extract.parser.ParsingReader;
import org.icij.extract.report.Reporter;
import org.icij.extract.spewer.Spewer;
import org.icij.extract.spewer.SpewerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A task is defined as both the extraction from a file and the output of extracted data.
 * Completion is only considered successful if both parts of the task complete with no exceptions.
 *
 * The final status of each task is saved to the reporter, if any is set.
 *
 * @since 1.0.0
 */
class ExtractionTask implements Runnable, Callable<Path> {

	private static final Logger logger = LoggerFactory.getLogger(ExtractionTask.class);

	protected final Path file;
	protected final Extractor extractor;
	protected final Spewer spewer;

	private final Reporter reporter;

	ExtractionTask(final Path file, final Extractor extractor, final Spewer spewer, final Reporter reporter) {
		this.file = file;
		this.extractor = extractor;
		this.spewer = spewer;
		this.reporter = reporter;
	}

	@Override
	public void run() {
		try {
			call();
		} catch (Exception e) {
			logger.error(String.format("Exception while consuming file: \"%s\".", file), e);
		}
	}

	@Override
	public Path call() throws Exception {

		// Check status in reporter. Skip if good.
		// Otherwise save status to registry and start a new job.
		if (null != reporter) {
			if (reporter.check(file, ExtractionResult.SUCCEEDED)) {
				logger.info(String.format("File already extracted; skipping: \"%s\".", file));
			} else {
				reporter.save(file, extractResult(file));
			}

			return file;
		}

		try {
			extract(file);

		// Catch exceptions that should be converted into warnings.
		} catch (IOException e) {
			final Throwable cause = e.getCause();

			if (null != cause && cause instanceof ExcludedMediaTypeException) {
				logger.warn(String.format("The document was not parsed because all of the parsers that handle it " +
						"were excluded: \"%s\".", file));
			} else {
				throw e;
			}
		}

		return file;
	}

	/**
	 * Send a file to the {@link Extractor}.
	 *
	 * @param file path of file to extract from
	 * @throws Exception if the extraction or output could not be completed
	 */
	private void extract(final Path file) throws Exception {
		final Metadata metadata = new Metadata();

		logger.info(String.format("Beginning extraction: \"%s\".", file));

		try (final ParsingReader reader = extractor.extract(file, metadata)) {
			spewer.write(file, metadata, reader);
		}
	}

	/**
	 * Send a file to the {@link Extractor} and return the result.
	 *
	 * @param file path of file to extract from
	 * @return The extraction result code.
	 */
	private ExtractionResult extractResult(final Path file) {
		ExtractionResult status = ExtractionResult.SUCCEEDED;

		try {
			extract(file);

		// SpewerException is thrown exclusively due to an output endpoint error.
		// It means that extraction succeeded, but the result could not be saved.
		} catch (SpewerException e) {
			logger.error(String.format("The extraction result could not be outputted: \"%s\".", file), e);
			status = ExtractionResult.NOT_SAVED;
		} catch (FileNotFoundException e) {
			logger.error(String.format("File not found: \"%s\". Skipping.", file), e);
			status = ExtractionResult.NOT_FOUND;
		} catch (IOException e) {

			// ParsingReader#read catches exceptions and wraps them in an IOException.
			final Throwable c = e.getCause();

			if (c instanceof ExcludedMediaTypeException) {
				status = ExtractionResult.EXCLUDED;
			} else if (c instanceof EncryptedDocumentException) {
				logger.warn(String.format("Skipping encrypted file: \"%s\".", file), e);
				status = ExtractionResult.NOT_DECRYPTED;

			// TIKA-198: IOExceptions thrown by parsers will be wrapped in a TikaException.
			// This helps us differentiate input stream exceptions from output stream exceptions.
			// https://issues.apache.org/jira/browse/TIKA-198
			} else if (c instanceof TikaException) {
				logger.error(String.format("The document could not be parsed: \"%s\".", file), e);
				status = ExtractionResult.NOT_PARSED;
			} else {
				logger.error(String.format("The document stream could not be read: \"%s\".", file), e);
				status = ExtractionResult.NOT_READ;
			}
		} catch (Exception e) {
			logger.error(String.format("Unknown exception during extraction or output: \"%s\".", file), e);
			status = ExtractionResult.NOT_CLEAR;
		}

		if (ExtractionResult.SUCCEEDED == status) {
			logger.info(String.format("Finished outputting file: \"%s\".", file));
		}

		return status;
	}
}
