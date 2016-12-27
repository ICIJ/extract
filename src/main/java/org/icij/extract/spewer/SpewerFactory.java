package org.icij.extract.spewer;

import org.apache.commons.cli.ParseException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.icij.extract.OutputType;
import org.icij.extract.extractor.Extractor;
import org.icij.extract.spewer.*;
import org.icij.net.http.PinnedHttpClientBuilder;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

/**
 * A factory class for creating {@link Spewer} instances from given commandline option values.
 *
 * @since 1.0.0
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 */
@Option(name = "outputType", description = "Set the output type. Either \"file\", \"stdout\" or \"solr\".",
		parameter = "type", code = "o")
@Option(name = "indexType", description = "Specify the index type. For now, the only valid value is " +
		"\"solr\" (the default).", parameter = "type")
@Option(name = "indexAddress", description = "Index core API endpoint address.", code = "s", parameter = "url")
@Option(name = "indexServerCertificate", description = "The index server's public certificate, used for" +
		" certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "indexVerifyHost", description = "Verify the index server's public certificate against " +
		"the specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
@OptionsClass(SolrSpewer.class)
@OptionsClass(FileSpewer.class)
@OptionsClass(PrintStreamSpewer.class)
public abstract class SpewerFactory {

	/**
	 * Create a new {@link Spewer} by parsing the given commandline parameters.
	 *
	 * @param options the options to parse
	 * @return A new spewer configured according to the given parameters.
	 * @throws ParseException When the commandline parameters cannot be read.
	 */
	public static Spewer createSpewer(final Options<String> options) throws ParseException {
		final OutputType outputType = options.get("outputType").parse().asEnum(OutputType::parse)
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
		final Extractor.EmbedHandling handling = options.get("embedHandling").parse().asEnum(Extractor
				.EmbedHandling::parse).orElse(Extractor.EmbedHandling.getDefault());

		// Calling #close on the SolrSpewer later on automatically closes these clients.
		final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(options.get("indexVerifyHost").value().orElse(null))
				.pinCertificate(options.get("indexServerCertificate").value().orElse(null))
				.build();
		final SolrClient solrClient = new HttpSolrClient.Builder(options.get("indexAddress").value().orElse
				("http://127.0.0.1:8983/solr/"))
				.withHttpClient(httpClient)
				.build();

		if (Extractor.EmbedHandling.SPAWN == handling) {
			return new MergingSolrSpewer(solrClient, fields);
		}

		return new SolrSpewer(solrClient, fields);
	}
}
