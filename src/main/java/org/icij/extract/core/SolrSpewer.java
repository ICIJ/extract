package org.icij.extract.core;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.File;
import java.io.Reader;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import org.apache.tika.metadata.Metadata;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.commons.io.IOUtils;

/**
 * Writes the text output from a {@link ParsingReader} to a Solr core.
 *
 * @since 1.0.0-beta
 */
public class SolrSpewer extends Spewer {
	public static final String DEFAULT_ID_FIELD = "id";
	public static final String DEFAULT_TEXT_FIELD = "content";
	public static final String DEFAULT_PATH_FIELD = "path";
	public static final String DEFAULT_METADATA_FIELD_PREFIX = "metadata_";

	private final SolrClient client;
	private final Semaphore commitSemaphore = new Semaphore(1);

	private String textField = DEFAULT_TEXT_FIELD;
	private String pathField = DEFAULT_PATH_FIELD;
	private String idField = DEFAULT_ID_FIELD;
	private String metadataFieldPrefix = DEFAULT_METADATA_FIELD_PREFIX;
	private MessageDigest idDigest = null;

	private final AtomicInteger pending = new AtomicInteger(0);
	private int commitInterval = 0;
	private int commitWithin = 0;

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

	public void setIdAlgorithm(final String algorithm) throws NoSuchAlgorithmException {
		this.idDigest = MessageDigest.getInstance(algorithm);
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

	public void finish() throws IOException {
		super.finish();

		// Commit any remaining files if autocommitting is enabled.
		if (commitInterval > 0) {
			commitPending(0);
		}

		client.close();

		if (client instanceof HttpSolrClient) {
			((CloseableHttpClient) ((HttpSolrClient) client).getHttpClient()).close();
		}
	}

	public void write(final Path file, final Metadata metadata, final Reader reader, final Charset outputEncoding)
		throws IOException, SpewerException {

		final String outputPath = filterOutputPath(file).toString();
		final SolrInputDocument document = new SolrInputDocument();
		final UpdateResponse response;

		setAtomic(document, textField, IOUtils.toString(reader));

		// Set the metadata.
		if (outputMetadata) {
			setAtomicMeta(document, metadata);
		}

		// Set the path on the path field.
		setAtomic(document, pathField, outputPath);

		// Set the ID.
		if (null != idField && null != idDigest) {
			document.setField(idField, DatatypeConverter.printHexBinary(idDigest
				.digest(outputPath.getBytes(outputEncoding))));
		}

		try {
			if (commitWithin > 0) {
				response = client.add(document);
			} else {
				response = client.add(document, commitWithin);
			}
		} catch (SolrServerException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: %s. There was server-side error.", file), e);
		} catch (IOException e) {
			throw new SpewerException(String.format("Unable to add file to Solr: %s. There was an error communicating with the server.", file), e);
		}

		logger.info(String.format("Document added to Solr in %dms: %s.", response.getElapsedTime(), file));
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

		// Don't rethrow. Commit errors are recovarable and the file was actually output sucessfully.
		} catch (SolrServerException e) {
			logger.log(Level.SEVERE, "Failed to commit to Solr. A server-side error to occurred.", e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to commit to Solr. There was an error communicating with the server.");
		} finally {
			commitSemaphore.release();
		}
	}

	private void setAtomicMeta(final SolrInputDocument document, final Metadata metadata) {
		for (String name : metadata.names()) {
			if (null != metadataFieldPrefix) {
				setAtomic(document, metadataFieldPrefix + name, metadata.get(name));
			} else {
				setAtomic(document, name, metadata.get(name));
			}
		}
	}

	private void setAtomic(final SolrInputDocument document, final String name, final String value) {
		final Map<String, String> atomic = new HashMap<String, String>();

		// Make an atomic update.
		// See: https://cwiki.apache.org/confluence/display/solr/Updating+Parts+of+Documents
		atomic.put("set", value);
		document.setField(name, atomic);
	}
}
