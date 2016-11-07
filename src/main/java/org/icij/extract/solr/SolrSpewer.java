package org.icij.extract.solr;

import org.icij.extract.core.IndexDefaults;
import org.icij.extract.core.Spewer;
import org.icij.extract.core.SpewerException;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import java.util.regex.Pattern;

import java.util.Locale;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Arrays;

import java.io.Reader;
import java.io.IOException;

import java.nio.file.Path;

import java.security.Security;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrServerException;

import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the text output from a {@link org.icij.extract.core.ParsingReader} to a Solr core.
 *
 * @since 1.0.0-beta
 */
public class SolrSpewer extends Spewer {
	private static final Pattern fieldName = Pattern.compile("[^A-Za-z0-9]");
	private static final Logger logger = LoggerFactory.getLogger(SolrSpewer.class);

	private final SolrClient client;
	private final Semaphore commitSemaphore = new Semaphore(1);

	private String textField = IndexDefaults.DEFAULT_TEXT_FIELD;
	private String pathField = IndexDefaults.DEFAULT_PATH_FIELD;
	private String idField = IndexDefaults.DEFAULT_ID_FIELD;
	private String metadataFieldPrefix = IndexDefaults.DEFAULT_METADATA_FIELD_PREFIX;
	private String idAlgorithm = IndexDefaults.DEFAULT_ID_ALGORITHM;

	private final AtomicInteger pending = new AtomicInteger(0);
	private int commitThreshold = 0;
	private Duration commitWithin = null;
	private boolean atomicWrites = false;
	private boolean fixDates = true;

	public SolrSpewer(final SolrClient client) {
		super();
		this.client = client;
	}

	public void setTextField(final String textField) {
		this.textField = textField;
	}

	public void setPathField(final String pathField) {
		this.pathField = pathField;
	}

	public void setIdField(final String idField) {
		this.idField = idField;
	}

	public void setIdAlgorithm(final String idAlgorithm) throws NoSuchAlgorithmException {
		if (null == idAlgorithm) {
			this.idAlgorithm = null;
		} else if (idAlgorithm.matches("^[a-zA-Z\\-\\d]+$") &&
			null != Security.getProviders("MessageDigest." + idAlgorithm)) {
			this.idAlgorithm = idAlgorithm;
		} else {
			throw new NoSuchAlgorithmException(String.format("No such algorithm: \"%s\".", idAlgorithm));
		}
	}

	public void setMetadataFieldPrefix(final String metadataFieldPrefix) {
		this.metadataFieldPrefix = metadataFieldPrefix;
	}

	public void setCommitThreshold(final int commitThreshold) {
		this.commitThreshold = commitThreshold;
	}

	/**
	 * Set how long Solr should wait before committing the added document, if at all.
	 *
	 * Note that a duration of zero will cause the document to be committed immediately. To disable this behaviour,
	 * pass {@literal null} as an argument.
	 *
	 * Automatic committing is disabled by default.
	 *
	 * @param commitWithin
	 */
	public void setCommitWithin(final Duration commitWithin) {
		this.commitWithin = commitWithin;
	}

	public void atomicWrites(final boolean atomicWrites) {
		this.atomicWrites = atomicWrites;
	}

	public boolean atomicWrites() {
		return atomicWrites;
	}

	public void fixDates(final boolean fixDates) {
		this.fixDates = fixDates;
	}

	public boolean fixDates() {
		return fixDates;
	}

	@Override
	public void close() throws IOException {

		// Commit any remaining files if auto-committing is enabled.
		if (commitThreshold > 0) {
			commitPending(0);
		}

		client.close();
		if (client instanceof HttpSolrClient) {
			((CloseableHttpClient) ((HttpSolrClient) client).getHttpClient()).close();
		}
	}

	public String generateId(final Path file) throws NoSuchAlgorithmException {
		return DatatypeConverter.printHexBinary(MessageDigest.getInstance(idAlgorithm)
			.digest(file.toString().getBytes(outputEncoding)));
	}

	@Override
	public void write(final Path file, final Metadata metadata, final Reader reader) throws IOException {
		final SolrInputDocument document = new SolrInputDocument();
		final UpdateResponse response;

		// Set extracted metadata fields supplied by Tika.
		if (outputMetadata) {
			setMetadataFieldValues(metadata, document);
		}

		// Set tags supplied by the caller.
		if (null != tags) {
			setTagFieldValues(document);
		}

		// Set the ID. Must never be written atomically.
		if (null != idField && null != idAlgorithm) {
			setIdFieldValue(file, document);
		}

		// Set the path field.
		setFieldValue(document, pathField, file.toString());

		// Set the parent path.
		if (file.getNameCount() > 1) {
			setFieldValue(document, IndexDefaults.DEFAULT_PARENT_PATH_FIELD, file.getParent().toString());
		}

		// Add the base type. De-duplicated. Eases faceting on type.
		setBaseTypeField(file, metadata, document);

		// Finally, set the text field containing the actual extracted text.
		setFieldValue(document, textField, IOUtils.toString(reader));

		try {
			if (null == commitWithin) {
				response = client.add(document);
			} else {
				response = client.add(document, Math.toIntExact(commitWithin.toMillis()));
			}
		} catch (SolrServerException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: \"%s\". " +
				"There was server-side error.", file), e);
		} catch (SolrException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: \"%s\". " +
				"HTTP error %d was returned.", file, e.code()), e);
		} catch (IOException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: \"%s\". " +
				"There was an error communicating with the server.", file), e);
		}

		logger.info(String.format("Document added to Solr in %dms: \"%s\".", response.getElapsedTime(), file));
		pending.incrementAndGet();

		// Autocommit if the interval is hit and enabled.
		if (commitThreshold > 0) {
			commitPending(commitThreshold);
		}
	}

	private void commitPending(final int threshold) {
		try {
			commitSemaphore.acquire();
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting to commit.", e);
			Thread.currentThread().interrupt();
			return;
		}

		if (pending.get() <= threshold) {
			commitSemaphore.release();
			return;
		}

		try {
			logger.warn("Committing to Solr.");
			final UpdateResponse response = client.commit();
			pending.set(0);
			logger.warn(String.format("Committed to Solr in %sms.", response.getElapsedTime()));

		// Don't rethrow. Commit errors are recoverable and the file was actually output successfully.
		} catch (SolrServerException e) {
			logger.error("Failed to commit to Solr. A server-side error to occurred.", e);
		} catch (SolrException e) {
			logger.error(String.format("Failed to commit to Solr. HTTP error %d was returned.", e.code()), e);
		} catch (IOException e) {
			logger.error("Failed to commit to Solr. There was an error communicating with the server.", e);
		} finally {
			commitSemaphore.release();
		}
	}

	private static final List<String> dateFieldNames = Arrays.asList(
		"dcterms:created",
		"dcterms:modified",
		"meta:save-date",
		"meta:creation-date",
		Metadata.MODIFIED,
		Metadata.DATE.getName(),
		Metadata.LAST_MODIFIED.getName(),
		Metadata.LAST_SAVED.getName(),
		Metadata.CREATION_DATE.getName());

	private void setMetadataFieldValues(final Metadata metadata, final SolrInputDocument document) {
		for (String name : metadata.names()) {
			String normalizedName = normalizeFieldName(name);

			if (null != metadataFieldPrefix) {
				normalizedName = metadataFieldPrefix + normalizedName;
			}

			if (metadata.isMultiValued(name)) {
				String[] values = metadata.getValues(name);

				// Remove duplicate content types.
				// Tika seems to add these sometimes, especially for RTF files.
				if (name.equals("Content-Type") && values.length > 1) {
					values = Arrays.stream(values).distinct().toArray(String[]::new);
				}

				setFieldValues(document, normalizedName, values);
			} else {
				setFieldValue(document, normalizedName, metadata.get(name));
			}
		}
	}

	private void setBaseTypeField(final Path file, final Metadata metadata, final SolrInputDocument document) {
		final Set<Object> baseTypes = new HashSet<>();

		for (String type : metadata.getValues(Metadata.CONTENT_TYPE)) {
			MediaType mediaType = MediaType.parse(type);

			if (null == mediaType) {
				logger.warn(String.format("Content type could not be parsed: \"%s\". Was: \"%s\".", file, type));
			} else {
				baseTypes.add(mediaType.getBaseType().toString());
			}
		}

		setFieldValues(document, IndexDefaults.DEFAULT_BASE_TYPE_FIELD, baseTypes.toArray());
	}

	private void setTagFieldValues(final SolrInputDocument document) {
		for (Map.Entry<String, String> tag : tags.entrySet()) {
			setFieldValue(document, normalizeFieldName(tag.getKey()), tag.getValue());
		}
	}

	private void setIdFieldValue(final Path file, final SolrInputDocument document) {
		try {
			document.setField(idField, generateId(file));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	private String normalizeFieldName(final String name) {
		return fieldName.matcher(name).replaceAll("_").toLowerCase(Locale.ROOT);
	}

	/**
	 * Set a value to a field on a Solr document.
	 *
	 * @param document the document to add the value to
	 * @param name the name of the field
	 * @param value the value
	 */
	private void setFieldValue(final SolrInputDocument document, final String name, final Object value) {
		if (atomicWrites) {
			final Map<String, Object> atomic = new HashMap<>();
			atomic.put("set", value);
			document.setField(name, atomic);
		} else {
			document.setField(name, value);
		}
	}

	/**
	 * Set a list of values to a multivalued field on a Solr document.
	 *
	 * @param document the document to add the values to
	 * @param name the name of the field
	 * @param values the values
	 */
	private void setFieldValues(final SolrInputDocument document, final String name, final Object[] values) {
		if (atomicWrites) {
			final Map<String, Object[]> atomic = new HashMap<>();
			atomic.put("set", values);
			document.setField(name, atomic);
		} else {
			document.setField(name, values);
		}
	}
}
