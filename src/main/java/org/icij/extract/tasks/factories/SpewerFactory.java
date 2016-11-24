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
import org.icij.extract.solr.SolrSpewer;
import org.icij.task.StringOptions;

import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
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
	public static Spewer createSpewer(final StringOptions options) throws ParseException {
		final OutputType outputType = options.get("output-type").asEnum(OutputType::parse).orElse(OutputType.STDOUT);
 		final String[] tags = options.get("tag").values();
		final Spewer spewer;

		if (OutputType.SOLR == outputType) {
			spewer = createSolrSpewer(options);
		} else if (OutputType.FILE == outputType) {
			spewer = createFileSpewer(options);
		} else {
			spewer = new PrintStreamSpewer(System.out);
		}

		options.get("output-metadata").asBoolean().ifPresent(spewer::outputMetadata);
		options.get("output-encoding").value().ifPresent(spewer::setOutputEncoding);

		if (null != tags) {
			setTags(spewer, tags);
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

	private static FileSpewer createFileSpewer(final StringOptions options) {
		final Extractor.OutputFormat outputFormat = options.get("output-format").asEnum(Extractor.OutputFormat::parse).orElse(null);
		final FileSpewer spewer = new FileSpewer(options.get("output-directory").asPath().orElse(Paths.get(".")));

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
	private static SolrSpewer createSolrSpewer(final StringOptions options) {
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

		options.get("atomic-writes").asBoolean().ifPresent(spewer::atomicWrites);
		options.get("fix-dates").asBoolean().ifPresent(spewer::fixDates);
		options.get("text-field").value().ifPresent(spewer::setTextField);
		options.get("path-field").value().ifPresent(spewer::setPathField);
		options.get("id-field").value().ifPresent(spewer::setIdField);
		options.get("metadata-prefix").value().ifPresent(spewer::setMetadataFieldPrefix);
		options.get("commit-interval").asInteger().ifPresent(spewer::setCommitThreshold);
		options.get("commit-within").asDuration().ifPresent(spewer::setCommitWithin);

		final Optional<String> idAlgorithm = options.get("id-algorithm").value();

		if (idAlgorithm.isPresent()) {
			try {
				spewer.setIdAlgorithm(idAlgorithm.get());
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalArgumentException(String.format("Hashing algorithm \"%s\" not available on this platform.",
						idAlgorithm.get()));
			}
		}

		return spewer;
	}
}
