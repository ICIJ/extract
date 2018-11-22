package org.icij.extract.spewer;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.icij.extract.OutputType;
import org.icij.extract.extractor.Extractor;
import org.icij.spewer.*;
import org.icij.spewer.http.PinnedHttpClientBuilder;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.apache.http.client.CredentialsProvider //NEW
import org.apache.http.auth.UsernamePasswordCredentials //NEW 
import static org.icij.extract.OutputType.STDOUT;

/**
 * A factory class for creating {@link Spewer} instances from given commandline option values.
 */
@Option(name = "outputType", description = "Set the output type. Either \"file\", \"stdout\", \"solr\".",
		parameter = "type", code = "o")
@Option(name = "indexType", description = "Specify the index type. Valid types are : solr", parameter = "type")
@Option(name = "indexAddress", description = "Index endpoint address.", code = "s", parameter = "url")
@Option(name = "indexServerCertificate", description = "The index server's public certificate, used for" +
		" certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "indexVerifyHost", description = "Verify the index server's public certificate against " +
		"the specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
@OptionsClass(FileSpewer.class)
@OptionsClass(PrintStreamSpewer.class)
@OptionsClass(SolrSpewer.class)
@OptionsClass(FieldNames.class)
public abstract class SpewerFactory {

	/**
	 * Create a new {@link Spewer} by parsing the given commandline parameters.
	 *
	 * @param options the options to parse
	 * @return A new spewer configured according to the given parameters.
	 * @throws Exception When the spewer cannot be created
	 */
	public static Spewer createSpewer(final Options<String> options) throws Exception {
		final OutputType outputType = options.get("outputType").parse().asEnum(OutputType::parse)
				.orElse(STDOUT);

		final FieldNames fields = new FieldNames().configure(options);
		final Spewer spewer;

		switch (outputType) {
            case REST: spewer = createRESTSpewer(options, fields);
                break;
            case FILE: spewer = new FileSpewer(fields);
                break;
			case SOLR: spewer = createSolrSpewer(options, fields);
				break;
            default: spewer = new PrintStreamSpewer(System.out, fields);
                break;
        }
		spewer.configure(options);
		return spewer;
	}

	private static CloseableHttpClient createHttpClient(final Options<String> options) {
		CredentialsProvider provider = new BasicCredentialsProvider(); //NEW
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
            "myusername","mypassword"); //NEW
		provider.setCredentials(AuthScope.ANY, credentials);                //NEW
		return PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(options.get("indexVerifyHost").value().orElse(null))
				..setDefaultCredentialsProvider(provider) //NEW
				.pinCertificate(options.get("indexServerCertificate").value().orElse(null))
				.build();
	}

	private static RESTSpewer createRESTSpewer(final Options<String> options, final FieldNames fields) {
		return new RESTSpewer(fields, createHttpClient(options), options.get("indexAddress").parse().asURI()
				.orElseThrow(IllegalArgumentException::new));
	}

	/**
	 * Create a new {@link SolrSpewer} by parsing the given options.
	 *
	 * @param options the options to parse
	 * @return A new spewer configured according to the given parameters.
	 */
	private static SolrSpewer createSolrSpewer(final Options<String> options, final FieldNames fields) {
		final Extractor.EmbedHandling handling = options.ifPresent("embedHandling", o -> o.parse().asEnum(Extractor
				.EmbedHandling::parse)).orElse(Extractor.EmbedHandling.getDefault());

		// Calling #close on the SolrSpewer later on automatically closes the HTTP client.
		final SolrClient solrClient = new HttpSolrClient.Builder(options.get("indexAddress").value().orElse
				("http://127.0.0.1:8983/solr/"))
				.withHttpClient(createHttpClient(options))
				.build();

		if (Extractor.EmbedHandling.SPAWN == handling) {
			return new MergingSolrSpewer(solrClient, fields);
		}

		return new SolrSpewer(solrClient, fields);
	}
}
