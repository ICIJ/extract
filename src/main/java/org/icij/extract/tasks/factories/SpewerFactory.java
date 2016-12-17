package org.icij.extract.tasks.factories;

import org.apache.commons.cli.ParseException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.icij.extract.OutputType;
import org.icij.extract.extractor.Extractor;
import org.icij.extract.spewer.*;
import org.icij.net.http.PinnedHttpClientBuilder;
import org.icij.task.Options;

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
	public static Spewer createSpewer(final Options<String> options) throws ParseException {
		final OutputType outputType = options.get("output-type").parse().asEnum(OutputType::parse)
				.orElse(OutputType.STDOUT);

		final FieldNames fields = new FieldNames().configure(options);
		final Spewer spewer;

		if (OutputType.SOLR == outputType) {
			spewer = createSolrSpewer(options, fields);
		} else if (OutputType.FILE == outputType) {
			spewer = new FileSpewer(fields);
		} else {
			spewer = new PrintStreamSpewer(System.out, fields);
		}

		spewer.configure(options);
		return spewer;
	}

	/**
	 * Create a new {@link SolrSpewer} by parsing the given options.
	 *
	 * @param options the options to parse
	 * @return A new spewer configured according to the given parameters.
	 */
	private static SolrSpewer createSolrSpewer(final Options<String> options, final FieldNames fields) {
		final Extractor.EmbedHandling handling = options.get("embed-handling").parse().asEnum(Extractor
				.EmbedHandling::parse).orElse(Extractor.EmbedHandling.getDefault());

		// Calling #close on the SolrSpewer later on automatically closes these clients.
		final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(options.get("index-verify-host").value().orElse(null))
				.pinCertificate(options.get("index-server-certificate").value().orElse(null))
				.build();
		final SolrClient solrClient = new HttpSolrClient.Builder(options.get("index-address").value().orElse
				("http://127.0.0.1:8983/solr/"))
				.withHttpClient(httpClient)
				.build();

		if (Extractor.EmbedHandling.SPAWN == handling) {
			return new MergingSolrSpewer(solrClient, fields);
		}

		return new SolrSpewer(solrClient, fields);
	}
}
