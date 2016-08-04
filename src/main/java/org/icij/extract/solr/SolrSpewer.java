package org.icij.extract.solr;

import org.icij.extract.core.Spewer;
import org.icij.extract.core.SpewerException;

import org.icij.extract.interval.TimeDuration;
import org.icij.extract.solr.SolrDefaults;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Locale;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import java.util.regex.Pattern;

import java.io.Reader;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import java.security.Security;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import org.apache.tika.metadata.Metadata;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrServerException;

import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.commons.io.IOUtils;

/**
 * Writes the text output from a {@link org.icij.extract.core.ParsingReader} to a Solr core.
 *
 * @since 1.0.0-beta
 */
public class SolrSpewer extends Spewer {
	private static final Pattern fieldName = Pattern.compile("[^A-Za-z0-9]");

	private final SolrClient client;
	private final Semaphore commitSemaphore = new Semaphore(1);

	private String textField = SolrDefaults.DEFAULT_TEXT_FIELD;
	private String pathField = SolrDefaults.DEFAULT_PATH_FIELD;
	private String idField = SolrDefaults.DEFAULT_ID_FIELD;
	private String metadataFieldPrefix = SolrDefaults.DEFAULT_METADATA_FIELD_PREFIX;
	private String idAlgorithm = null;

	private final AtomicInteger pending = new AtomicInteger(0);
	private int commitInterval = 0;
	private int commitWithin = 0;
	private boolean atomicWrites = false;
	private boolean fixDates = true;

	/**
	 * Normalize metadata field names.
	 *
	 * Field names must consist of alphanumeric or underscore characters only.
	 *
	 * @param name metadata field name
	 * @param prefix metadata field name prefix
	 * @return The result code.
	 */
	public static String normalizeName(final String name, final String prefix) {
		String normalized = fieldName.matcher(name).replaceAll("_").toLowerCase(Locale.ROOT);

		if (null != prefix) {
			normalized = prefix + normalized;
		}

		return normalized;
	}

	public SolrSpewer(final Logger logger, final SolrClient client) {
		super(logger);
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

	public void setCommitInterval(final int commitInterval) {
		this.commitInterval = commitInterval;
	}

	public void setCommitWithin(final int commitWithin) {
		this.commitWithin = commitWithin;
	}

	public void setCommitWithin(final String duration) {
		setCommitWithin((int) TimeDuration.parseTo(duration, TimeUnit.SECONDS));
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

	public void finish() throws IOException {
		super.finish();

		// Commit any remaining files if auto-committing is enabled.
		if (commitInterval > 0) {
			commitPending(0);
		}

		client.close();

		if (client instanceof HttpSolrClient) {
			((CloseableHttpClient) ((HttpSolrClient) client).getHttpClient()).close();
		}
	}

	public String generateId(final Path path) throws NoSuchAlgorithmException {
		return DatatypeConverter.printHexBinary(MessageDigest.getInstance(idAlgorithm)
			.digest(path.toString().getBytes(outputEncoding)));
	}

	public void write(final Path path, final Metadata metadata, final Reader reader) throws IOException {

		final SolrInputDocument document = new SolrInputDocument();
		final UpdateResponse response;

		// Set the metadata.
		if (outputMetadata) {
			filterMetadata(path, metadata);
			setMetadataFields(document, metadata);
		}

		if (null != tags) {
			for (Map.Entry<String, String> tag : tags.entrySet()) {
				setField(document, tag.getKey(), tag.getValue());
			}
		}

		// Set the path on the path field.
		setField(document, pathField, path.toString());
		setField(document, textField, IOUtils.toString(reader));

		// Set the ID. Must never be written atomically.
		if (null != idField && null != idAlgorithm) {
			try {
				document.setField(idField, generateId(path));
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException(e);
			}
		}

		try {
			if (commitWithin > 0) {
				response = client.add(document);
			} else {
				response = client.add(document, commitWithin);
			}
		} catch (SolrServerException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: \"%s\". " +
				"There was server-side error.", path), e);
		} catch (SolrException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: \"%s\". " +
				"HTTP error %d was returned.", path, e.code()), e);
		} catch (IOException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: \"%s\". " +
				"There was an error communicating with the server.", path), e);
		}

		logger.info(String.format("Document added to Solr in %dms: \"%s\".", response.getElapsedTime(), path));
		pending.incrementAndGet();

		// Autocommit if the interval is hit and enabled.
		if (commitInterval > 0) {
			commitPending(commitInterval);
		}
	}

	private void commitPending(final int threshold) {
		commitSemaphore.acquireUninterruptibly();
		if (pending.get() <= threshold) {
			commitSemaphore.release();
			return;
		}

		try {
			logger.info("Committing to Solr.");
			final UpdateResponse response = client.commit();
			pending.set(0);
			logger.info("Committed to Solr in " + response.getElapsedTime() + "ms.");

		// Don't rethrow. Commit errors are recoverable and the file was actually output successfully.
		} catch (SolrServerException e) {
			logger.log(Level.SEVERE, "Failed to commit to Solr. A server-side error to occurred.", e);
		} catch (SolrException e) {
			logger.log(Level.SEVERE, String.format("Failed to commit to Solr. HTTP error %d was returned.", e.code()), e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to commit to Solr. There was an error communicating with the server.", e);
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

	private void setMetadataFields(final SolrInputDocument document, final Metadata metadata) {
		for (String name : metadata.names()) {
			String[] values = metadata.getValues(name);

			// Remove duplicate content types.
			// Tika seems to add these sometimes, especially for RTF files.
			if (name.equals("Content-Type") && values.length > 1) {
				values = Arrays.asList(values).stream().distinct().toArray(String[]::new);
			}

			setMetadataField(document, name, values);
		}
	}

	private void setMetadataField(final SolrInputDocument document, final String name,
		final String[] values) {
		for (String value : values) {
			if (fixDates && dateFieldNames.contains(name) && !value.endsWith("Z")) {
				value = value + "Z";
			}

			if (values.length > 1) {
				addField(document, normalizeName(name), value);
			} else {
				setField(document, normalizeName(name), value);
			}
		}
	}

	private String normalizeName(final String name) {
		return normalizeName(name, metadataFieldPrefix);
	}

	private Map<String, String> createAtomic(final String value) {
		final Map<String, String> atomic = new HashMap<>();

		atomic.put("set", value);
		return atomic;
	}

	private void addField(final SolrInputDocument document, final String name, final String value) {
		if (atomicWrites) {
			document.addField(name, createAtomic(value));
		} else {
			document.addField(name, value);
		}
	}

	private void setField(final SolrInputDocument document, final String name, final String value) {
		if (atomicWrites) {
			document.setField(name, createAtomic(value));
		} else {
			document.setField(name, value);
		}
	}
}
