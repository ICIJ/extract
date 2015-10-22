package org.icij.extract.solr;

import org.icij.extract.solr.SolrDefaults;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;

public class SolrRehashConsumer extends SolrMachineConsumer {

	private final SolrClient client;
	private final String idAlgorithm;
	private Pattern pattern = null;
	private String replacement = "";
	private Charset outputEncoding = StandardCharsets.UTF_8;
	private String pathField = SolrDefaults.DEFAULT_PATH_FIELD;

	public SolrRehashConsumer(final Logger logger, final SolrClient client, final String idAlgorithm) {
		super(logger);
		this.client = client;
		this.idAlgorithm = idAlgorithm;
	}

	public void setOutputEncoding(final Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public void setOutputEncoding(final String outputEncoding) {
		setOutputEncoding(Charset.forName(outputEncoding));
	}

	public void setPathField(final String pathField) {
		this.pathField = pathField;
	}

	public void setPattern(final String pattern) {
		this.pattern = Pattern.compile(pattern);
	}

	public void setReplacement(final String replacement) {
		this.replacement = replacement;
	}

	@Override
	protected void consume(final SolrDocument input) throws SolrServerException, IOException, NoSuchAlgorithmException {
		final SolrInputDocument output = ClientUtils.toSolrInputDocument(input);
		final String inputPath = (String) input.getFieldValue(pathField);
		final String outputPath;

		if (null != pattern && null != replacement) {
			outputPath = pattern.matcher(inputPath).replaceAll(replacement);
		} else {
			outputPath = inputPath;
		}

		final String inputId = (String) input.getFieldValue(idField);
		final String outputId = DatatypeConverter.printHexBinary(MessageDigest.getInstance(idAlgorithm)
			.digest(outputPath.getBytes(outputEncoding)));

		// Only replace if the IDs are different.
		if (!inputId.equals(outputId)) {
			output.setField("_version_", "0"); // The document may or may not exist.
			output.setField(idField, outputId);
			output.setField(pathField, outputPath);

			logger.info(String.format("Replacing path \"%s\" with \"%s\" and rehashing ID from %s to %s.",
				inputPath, outputPath, inputId, outputId));
			client.add(output);
			client.deleteById((String) input.getFieldValue(idField));
		}
	}
}
