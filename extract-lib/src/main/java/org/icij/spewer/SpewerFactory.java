package org.icij.spewer;

import org.apache.http.impl.client.CloseableHttpClient;
import org.icij.extract.OutputType;
import org.icij.spewer.http.PinnedHttpClientBuilder;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import static org.icij.extract.OutputType.STDOUT;

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
				.orElse(STDOUT);

		final FieldNames fields = new FieldNames().configure(options);
		final Spewer spewer;

		switch (outputType) {
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


}
