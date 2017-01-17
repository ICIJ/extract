package org.icij.extract.spewer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.Reader;
import java.io.IOException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrServerException;

import org.apache.http.impl.client.CloseableHttpClient;

import org.icij.extract.document.Document;
import org.icij.extract.document.EmbeddedDocument;
import org.icij.extract.parser.ParsingReader;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the text output from a {@link ParsingReader} to a Solr core.
 *
 * @since 1.0.0-beta
 */
@Option(name = "commitInterval", description = "Commit to the index every time the specified number of " +
		"documents is added. Disabled by default. Consider using the \"autoCommit\" \"maxDocs\" directive in your " +
		"Solr update handler configuration instead.", parameter = "number")
@Option(name = "commitWithin", description = "Instruct Solr to automatically commit a document after the " +
		"specified amount of time has elapsed since it was added. Disabled by default. Consider using the " +
		"\"autoCommit\" \"maxTime\" directive in your Solr update handler configuration instead.", parameter =
		"duration")
@Option(name = "atomicWrites", description = "Make atomic updates to the index. If your schema contains " +
		"fields that are not included in the payload, this prevents their values, if any, from being erased.")
public class SolrSpewer extends Spewer {
	private static final Logger logger = LoggerFactory.getLogger(SolrSpewer.class);

	protected final SolrClient client;

	private final Semaphore commitSemaphore = new Semaphore(1);

	private final AtomicInteger pending = new AtomicInteger(0);
	private int commitThreshold = 0;
	private Duration commitWithin = null;
	private boolean atomicWrites = false;

	public SolrSpewer(final SolrClient client, final FieldNames fields) {
		super(fields);
		this.client = client;
	}

	public SolrSpewer configure(final Options<String> options) {
		super.configure(options);

		options.get("atomicWrites").parse().asBoolean().ifPresent(this::atomicWrites);
		options.get("commitInterval").parse().asInteger().ifPresent(this::setCommitThreshold);
		options.get("commitWithin").parse().asDuration().ifPresent(this::setCommitWithin);

		return this;
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
	 * @param commitWithin the duration that each document added will be committed within
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

	@Override
	public void write(final Document document, final Reader reader) throws IOException {
		final SolrInputDocument inputDocument = prepareDocument(document, reader);
		final UpdateResponse response;

		try {
			response = write(document, inputDocument);
		} catch (SolrServerException e) {
			throw new SpewerException(String.format("Unable to add document to Solr: \"%s\". " +
					"There was server-side error.", document), e);
		} catch (SolrException e) {
			throw new SpewerException(String.format("Unable to add document to Solr: \"%s\". " +
					"HTTP error %d was returned.", document, e.code()), e);
		} catch (IOException e) {
			throw new SpewerException(String.format("Unable to add document to Solr: \"%s\". " +
					"There was an error communicating with the server.", document), e);
		}

		logger.info(String.format("Document added to Solr in %dms: \"%s\".", response.getElapsedTime(), document));
		pending.incrementAndGet();

		// Autocommit if the interval is hit and enabled.
		if (commitThreshold > 0) {
			commitPending(commitThreshold);
		}
	}

	@Override
	public void writeMetadata(final Document document) {
		throw new UnsupportedOperationException();
	}

	protected UpdateResponse write(final Document document, final SolrInputDocument inputDocument) throws
			IOException, SolrServerException {
		if (null != commitWithin) {
			return client.add(inputDocument, Math.toIntExact(commitWithin.toMillis()));
		}

		return client.add(inputDocument);
	}

	private SolrInputDocument prepareDocument(final Document document, final Reader reader) throws IOException {
		final SolrInputDocument inputDocument = new SolrInputDocument();

		// Set extracted metadata fields supplied by Tika.
		if (outputMetadata) {
			setMetadataFieldValues(document.getMetadata(), inputDocument);
		}

		// Set tags supplied by the caller.
		tags.forEach((key, value)-> setFieldValue(inputDocument, fields.forTag(key), value));

		// Set the ID. Must never be written atomically.
		if (null != fields.forId() && null != document.getId()) {
			inputDocument.setField(fields.forId(), document.getId());
		}

		// Add the base type. De-duplicated. Eases faceting on type.
		setFieldValue(inputDocument, fields.forBaseType(), Arrays.stream(document.getMetadata()
				.getValues(Metadata.CONTENT_TYPE)).map((type)-> {
			final MediaType mediaType = MediaType.parse(type);

			if (null == mediaType) {
				logger.warn(String.format("Content type could not be parsed: \"%s\". Was: \"%s\".", document, type));
				return type;
			}

			return mediaType.getBaseType().toString();
		}).toArray(String[]::new));

		// Set the path field.
		if (null != fields.forPath()) {
			setFieldValue(inputDocument, fields.forPath(), document.getPath().toString());
		}

		// Set the parent path field.
		if (null != fields.forParentPath() && document.getPath().getNameCount() > 1) {
			setFieldValue(inputDocument, fields.forParentPath(), document.getPath().getParent().toString());
		}

		// Finally, set the text field containing the actual extracted text.
		setFieldValue(inputDocument, fields.forText(), toString(reader));

		// Add embedded documents as child documents.
		for (EmbeddedDocument embed : document.getEmbeds()) {
			try (final Reader embedReader = embed.getReader()) {
				inputDocument.addChildDocument(prepareDocument(embed, embedReader));
			}
		}

		return inputDocument;
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

	private void setMetadataFieldValues(final Metadata metadata, final SolrInputDocument document) throws SpewerException {
		applyMetadata(metadata, (name, value)-> setFieldValue(document, name, value), (name, values)-> setFieldValue
				(document, name, values));
	}

	/**
	 * Set a value to a field on a Solr document.
	 *
	 * @param document the document to add the value to
	 * @param name the name of the field
	 * @param value the value
	 */
	void setFieldValue(final SolrInputDocument document, final String name, final String value) {
		if (atomicWrites) {
			final Map<String, String> atomic = new HashMap<>();
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
	void setFieldValue(final SolrInputDocument document, final String name, final String[] values) {
		if (atomicWrites) {
			final Map<String, String[]> atomic = new HashMap<>();
			atomic.put("set", values);
			document.setField(name, atomic);
		} else {
			document.setField(name, values);
		}
	}
}
