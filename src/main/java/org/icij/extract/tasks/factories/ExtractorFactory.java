package org.icij.extract.tasks.factories;

import org.icij.extract.core.Extractor;
import org.icij.task.StringOptions;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

/**
 * Factory methods for creating {@link Extractor} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ExtractorFactory {

	public static Extractor createExtractor(final StringOptions options) {
		final Extractor extractor = new Extractor();
		final Optional<Extractor.OutputFormat> outputFormat = options.get("output-format").asEnum(Extractor
				.OutputFormat::parse);
		final Optional<Extractor.EmbedHandling> embedHandling = options.get("embed-handling").asEnum(Extractor
				.EmbedHandling::parse);
		final Optional<String> ocrLanguage = options.get("ocr-language").value();
		final Optional<Duration> ocrTimeout = options.get("ocr-timeout").asDuration();
		final Optional<Path> workingDirectory = options.get("working-directory").asPath();

		if (outputFormat.isPresent()) {
			extractor.setOutputFormat(outputFormat.get());
		}

		if (embedHandling.isPresent()) {
			extractor.setEmbedHandling(embedHandling.get());
		}

		if (ocrLanguage.isPresent()) {
			extractor.setOcrLanguage(ocrLanguage.get());
		}

		if (ocrTimeout.isPresent()) {
			extractor.setOcrTimeout(ocrTimeout.get());
		}

		if (options.get("ocr").off()) {
			extractor.disableOcr();
		}

		if (workingDirectory.isPresent()) {
			extractor.setWorkingDirectory(workingDirectory.get());
		}

		return extractor;
	}
}
