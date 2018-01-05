package org.icij.extract.spewer;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icij.extract.OutputType;
import org.icij.extract.extractor.Extractor;
import org.icij.net.http.PinnedHttpClientBuilder;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

/**
 * A factory class for creating {@link Spewer} instances from given commandline option values.
 *
 * @since 1.0.0
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 */
@Option(name = "outputType", description = "Set the output type. Either \"file\", \"stdout\", \"solr\" or \"elasticsearch\".",
		parameter = "type", code = "o")
@Option(name = "indexType", description = "Specify the index type. Valid types are : solr, elasticsearch", parameter = "type")
@Option(name = "indexAddress", description = "Index endpoint address.", code = "s", parameter = "url")
@Option(name = "indexServerCertificate", description = "The index server's public certificate, used for" +
		" certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.", parameter = "path")
@Option(name = "indexVerifyHost", description = "Verify the index server's public certificate against " +
		"the specified host. Use the wildcard \"*\" to disable verification.", parameter = "hostname")
@OptionsClass(SolrSpewer.class)
@OptionsClass(MergingSolrSpewer.class)
@OptionsClass(FileSpewer.class)
@OptionsClass(PrintStreamSpewer.class)
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
				.orElse(OutputType.STDOUT);

		final FieldNames fields = new FieldNames().configure(options);
		final Spewer spewer;

		switch (outputType) {
            case SOLR: spewer = createSolrSpewer(options, fields);
                break;
            case ELASTICSEARCH: spewer = createElasticsearchSpewer(options, fields);
                break;
            case REST: spewer = createRESTSpewer(options, fields);
                break;
            case FILE: spewer = new FileSpewer(fields);
                break;
            default: spewer = new PrintStreamSpewer(System.out, fields);
                break;
        }
		spewer.configure(options);
		return spewer;
	}

	private static CloseableHttpClient createHttpClient(final Options<String> options) {
		return PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(options.get("indexVerifyHost").value().orElse(null))
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

	private static ElasticsearchSpewer createElasticsearchSpewer(final Options<String> options, final FieldNames fields)
            throws UnknownHostException {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        InetAddress esAddress = InetAddress.getByName("localhost");
        int esPort = 9300;
        Optional<String> indexAddress = options.get("indexAddress").value();
        if (indexAddress.isPresent()) {
            esAddress = InetAddress.getByName(indexAddress.get().split(":")[0]);
            esPort = Integer.parseInt(indexAddress.get().split(":")[1]);
        }

        Settings settings = Settings.builder().put("cluster.name", "datashare").build();
        Client client = new PreBuiltTransportClient(settings).addTransportAddress(
                new TransportAddress(esAddress, esPort));
        return new ElasticsearchSpewer(client, fields);
    }
}
