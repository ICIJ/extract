package org.icij.extract.solr;

import java.util.Map;
import java.util.HashMap;

import java.util.Objects;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Paths;
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

/**
 * A consumer that recalculates ID hashes of documents after a simple
 * regular expression replacement on the path.
 *
 * This is useful when you want to change the paths of documents that
 * have already been added to Solr.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class SolrRehashConsumer extends SolrMachineConsumer {

	private final SolrClient client;
	private final String idAlgorithm;
	private Pattern pattern = null;
	private String replacement = "";
	private Charset outputEncoding = StandardCharsets.UTF_8;
	private String pathField = SolrDefaults.DEFAULT_PATH_FIELD;
	private String metadataFieldPrefix = SolrDefaults.DEFAULT_METADATA_FIELD_PREFIX;

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

	public void setMetadataFieldPrefix(final String metadataFieldPrefix) {
		this.metadataFieldPrefix = metadataFieldPrefix;
	}

	@Override
	protected void consume(final SolrDocument input) throws SolrServerException, IOException, NoSuchAlgorithmException {
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
		final String outputPathParent = Objects.toString(Paths.get(outputPath).getParent(), "");

		// If the hash hasn't changed, skip.
		// Skip by comparing the hash values and not the paths because the algorithm might have been changed.
		if (inputId.equals(outputId)) {
			return;
		}

		final SolrInputDocument output = new SolrInputDocument();

		for (String name: input.getFieldNames()) {
			output.addField(name, input.getFieldValue(name));
		}

		output.setField("_version_", "-1"); // The document must not exist.
		output.setField(idField, outputId);
		output.setField(pathField, outputPath);
		output.setField(SolrDefaults.DEFAULT_PARENT_PATH_FIELD, outputPathParent);

		logger.info(String.format("Replacing path \"%s\" with \"%s\" and rehashing ID from \"%s\" to \"%s\".",
				inputPath, outputPath, inputId, outputId));
		client.add(output);
		client.deleteById(inputId);
	}

	private Map<String, String> createAtomic(final String value) {
		final Map<String, String> atomic = new HashMap<>();

		atomic.put("set", value);
		return atomic;
	}
}
