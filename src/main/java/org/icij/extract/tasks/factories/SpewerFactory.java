package org.icij.extract.tasks.factories;

import org.apache.commons.cli.ParseException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.icij.extract.OutputType;
import org.icij.extract.core.Extractor;
import org.icij.extract.core.FileSpewer;
import org.icij.extract.core.PrintStreamSpewer;
import org.icij.extract.core.Spewer;
import org.icij.net.http.PinnedHttpClientBuilder;
import org.icij.extract.core.IndexDefaults;
import org.icij.extract.solr.SolrSpewer;
import org.icij.task.DefaultOption;

import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Optional;

/**
 * A factory class for creating {@link Spewer} instances from given commandline option values.
 *
 * @since 1.0.0
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 */
public abstract class SpewerFactory {

	/**
	 * Create a new {@link Spewer} by parsing the given commandline parameters.
	 *
	 * @param options the options to parse
	 * @return A new spewer configured according to the given parameters.
	 * @throws ParseException When the commandline parameters cannot be read.
	 */
	public static Spewer createSpewer(final DefaultOption.Set options) throws ParseException {
		final OutputType outputType = options.get("output-type").set(OutputType::parse).orElse(OutputType.STDOUT);
		final Optional<String> outputEncoding = options.get("output-encoding").value();
 		final String[] tags = options.get("tag").values();
		final Spewer spewer;

		if (OutputType.SOLR == outputType) {
			spewer = createSolrSpewer(options);
		} else if (OutputType.FILE == outputType) {
			spewer = createFileSpewer(options);
		} else {
			spewer = new PrintStreamSpewer(System.out);
		}

		if (options.get("output-metadata").off()) {
			spewer.outputMetadata(false);
		}

		if (null != tags) {
			setTags(spewer, tags);
		}

		if (outputEncoding.isPresent()) {
			spewer.setOutputEncoding(outputEncoding.get());
		}

		return spewer;
	}

	private static void setTags(final Spewer spewer, final String[] tags) {
		for (String tag : tags) {
			String[] pair = tag.split(":", 2);

			if (2 == pair.length) {
				spewer.setTag(pair[0], pair[1]);
			} else {
				throw new IllegalArgumentException(String.format("Invalid tag pair: \"%s\".", tag));
			}
		}
	}

	private static FileSpewer createFileSpewer(final DefaultOption.Set options) {
		final Extractor.OutputFormat outputFormat = options.get("output-format").set(Extractor.OutputFormat::parse).orElse(null);
		final FileSpewer spewer = new FileSpewer(options.get("output-directory").path().orElse(Paths.get(".")));

		if (null != outputFormat && outputFormat.equals(Extractor.OutputFormat.HTML)) {
			spewer.setOutputExtension("html");
		}

		return spewer;
	}

	/**
	 * Create a new {@link SolrSpewer} by parsing the given options.
	 *
	 * @param options the options to parse
	 * @return A new spewer configured according to the given parameters.
	 */
	private static SolrSpewer createSolrSpewer(final DefaultOption.Set options) {
		final SolrSpewer spewer;

		// Calling #close on the SolrSpewer later on automatically closes these clients.
		final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(options.get("index-verify-host").value().orElse(null))
				.pinCertificate(options.get("index-server-certificate").value().orElse(null))
				.build();

		spewer = new SolrSpewer(new HttpSolrClient.Builder(options.get("index-address").value().orElse
				("http://127.0.0.1:8983/solr/"))
				.withHttpClient(httpClient)
				.build());

		spewer.atomicWrites(options.get("atomic-writes").on());
		spewer.fixDates(options.get("raw-dates").on());

		final Optional<String> textField = options.get("text-field").value();
		final Optional<String> pathField = options.get("path-field").value();
		final Optional<String> idField = options.get("id-field").value();
		final Optional<String> metadataPrefix = options.get("metadata-prefix").value();
		final Optional<Integer> commitInterval = options.get("commit-interval").integer();
		final Optional<Duration> commitWithin = options.get("commit-within").duration();
		final String idAlgorithm = options.get("id-algorithm").value().orElse(IndexDefaults.DEFAULT_ID_ALGORITHM);

		if (textField.isPresent()) {
			spewer.setTextField(textField.get());
		}

		if (pathField.isPresent()) {
			spewer.setPathField(pathField.get());
		}

		if (idField.isPresent()) {
			spewer.setIdField(idField.get());
		}

		try {
			spewer.setIdAlgorithm(idAlgorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException(String.format("Hashing algorithm \"%s\" not available on this platform.",
					idAlgorithm));
		}

		if (metadataPrefix.isPresent()) {
			spewer.setMetadataFieldPrefix(metadataPrefix.get());
		}

		if (commitInterval.isPresent()) {
			spewer.setCommitThreshold(commitInterval.get());
		}

		if (commitWithin.isPresent()) {
			spewer.setCommitWithin(commitWithin.get());
		}

		return spewer;
	}
}
