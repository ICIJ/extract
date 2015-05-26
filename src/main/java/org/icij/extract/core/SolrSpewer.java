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
	public static final String DEFAULT_FIELD = "text";
	public static final int DEFAULT_INTERVAL = 10;

	private final SolrClient client;
	private final Semaphore commitSemaphore = new Semaphore(1);

	private String field = DEFAULT_FIELD;

	private int pending = 0;
	private int interval = DEFAULT_INTERVAL;

	public SolrSpewer(Logger logger, SolrClient client) {
		super(logger);
		this.client = client;
	}

	public void setField(String field) {
		this.field = field;
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

	public void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException {
		final SolrInputDocument document = new SolrInputDocument();
		final Map<String, String> text = new HashMap<String, String>();
		final UpdateResponse response;

		try {
			text.put("set", IOUtils.toString(reader));
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		// Make an atomic update.
		// See: https://cwiki.apache.org/confluence/display/solr/Updating+Parts+of+Documents
		document.setField(field, text);

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
