package org.icij.extract.core;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import java.util.logging.Logger;

import java.util.concurrent.Semaphore;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import org.apache.tika.parser.ParsingReader;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import org.apache.commons.io.IOUtils;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SolrSpewer extends Spewer {
	public static final String DEFAULT_TEXT_FIELD = "text";
	public static final String DEFAULT_PATH_FIELD = "path";

	public static final int DEFAULT_INTERVAL = 10;

	private final SolrClient client;
	private final Semaphore commitSemaphore = new Semaphore(1);

	private String textField = DEFAULT_TEXT_FIELD;
	private String pathField = DEFAULT_PATH_FIELD;
	private String idField = null;
	private MessageDigest idDigest = null;

	private volatile int pending = 0;
	private int interval = DEFAULT_INTERVAL;

	public SolrSpewer(Logger logger, SolrClient client) {
		super(logger);
		this.client = client;
	}

	public void setTextField(String textField) {
		this.textField = textField;
	}

	public void setPathField(String pathField) {
		this.pathField = pathField;
	}

	public void setIdField(String idField, String algorithm) throws NoSuchAlgorithmException {
		this.idDigest = MessageDigest.getInstance(algorithm);
		this.idField = idField;
	}

	public void setCommitInterval(int interval) {
		this.interval = interval;
	}

	public void finish() throws IOException {
		super.finish();

		commitSemaphore.acquireUninterruptibly();
		if (pending > 0) {
			commitAndRelease();
		}
	}

	public void write(final Path file, final ParsingReader reader, final Charset outputEncoding) throws IOException {
		final SolrInputDocument document = new SolrInputDocument();
		final UpdateResponse response;

		try {
			setAtomic(document, textField, IOUtils.toString(reader));
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		setAtomic(document, pathField, file.toString());

		if (null != idField && null != idDigest) {
			final byte[] hash = idDigest.digest(file.toString().getBytes());

			setAtomic(document, idField, DatatypeConverter.printHexBinary(hash));
		}

		try {
			response = client.add(document);
		} catch (SolrServerException e) {
			throw new IOException("Error while adding to Solr: " + file + ".", e);
		}

		logger.info("Document added to Solr in " + response.getElapsedTime() + "ms: " + file + ".");

		commitSemaphore.acquireUninterruptibly();
		pending++;
		if (pending > interval) {
			commitAndRelease();
		}
	}

	private void setAtomic(final SolrInputDocument document, final String name, final String value) {
		final Map<String, String> atomic = new HashMap<String, String>();

		// Make an atomic update.
		// See: https://cwiki.apache.org/confluence/display/solr/Updating+Parts+of+Documents
		atomic.put("set", value);
		document.setField(name, value);
	}

	private void commitAndRelease() throws IOException {
		try {
			commit();
		} catch (IOException e) {
			throw e;
		} finally {
			commitSemaphore.release();
		}
	}

	private void commit() throws IOException {
		final UpdateResponse response;

		logger.info("Committing to Solr.");

		try {
			response = client.commit();
		} catch (SolrServerException e) {
			throw new IOException("Error while committing to Solr.", e);
		}

		pending = 0;

		logger.info("Committed to Solr in " + response.getElapsedTime() + "ms.");
	}
}
