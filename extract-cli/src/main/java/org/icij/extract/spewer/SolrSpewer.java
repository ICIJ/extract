package org.icij.extract.spewer;

import org.apache.commons.io.TaggedIOException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.parser.ParsingReader;
import org.icij.spewer.FieldNames;
import org.icij.spewer.MetadataTransformer;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
@Option(name = "testoption", description = "Another option " +
		"continues", parameter="number")
public class SolrSpewer extends Spewer implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(SolrSpewer.class);
	private static final long serialVersionUID = -8455227685165065698L;

	protected final SolrClient client;

	private final Semaphore commitSemaphore = new Semaphore(1);

	private final AtomicInteger pending = new AtomicInteger(0);
	private int commitThreshold = 0;
	private Duration commitWithin = null;

	private boolean atomicWrites = false;
	private boolean dump = true;

	public SolrSpewer(final SolrClient client, final FieldNames fields) {
		super(fields);
		this.client = client;
	}

	public SolrSpewer configure(final Options<String> options) {
		super.configure(options);
//                System.out.println(options);
		options.get("atomicWrites").parse().asBoolean().ifPresent(this::atomicWrites);
		options.get("commitInterval").parse().asInteger().ifPresent(this::setCommitThreshold);
		options.get("commitWithin").parse().asDuration().ifPresent(this::setCommitWithin);
                    System.out.println(options);
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

	public void dump(final boolean dump) {
		this.dump = dump;
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
	public void write(final TikaDocument tikaDocument, final Reader reader) throws IOException {
		final SolrInputDocument inputDocument = prepareDocument(tikaDocument, reader, 0);
		UpdateResponse response;

		response = write(tikaDocument, inputDocument);
		logger.info("TikaDocument added to Solr in {}ms: \"{}\".", response.getElapsedTime(), tikaDocument);

		try {

			// We need to deleted the "fake" child documents so that there are no orphans.
			response = client.deleteByQuery(String.format("%s:\"%s\" AND %s:[1 TO *]", fields.forRoot(),
					tikaDocument.getId(), fields.forLevel()));
		} catch (final SolrServerException e) {
			throw new IOException(e);
		}

		logger.info("Deleted old child documents in {}ms: \"{}\".", response.getElapsedTime(), tikaDocument);

		if (tikaDocument.hasEmbeds()) {
			for (EmbeddedTikaDocument childDocument : tikaDocument.getEmbeds()) {
				write(childDocument, 1, tikaDocument, tikaDocument);
			}

			logger.info("Wrote child documents: \"{}\".", tikaDocument);
		}
	}

	@Override
	public void writeMetadata(final TikaDocument tikaDocument) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TikaDocument[] write(final Path path) throws IOException, ClassNotFoundException {
		try (final InputStream fis = Files.newInputStream(path);
				final ObjectInputStream in = new ObjectInputStream(path.toString().endsWith(".gz") ?
						new GZIPInputStream(fis) : fis)) {
			final SolrInputDocument inputDocument = (SolrInputDocument) in.readObject();

			in.close();

			final TikaDocument[] tikaDocuments = inputDocument
					.getFieldValues(fields.forPath()).stream().map(p -> new TikaDocument(inputDocument
					.getFieldValue(fields.forId()).toString(), null, Paths.get(p.toString()), null))
			.toArray(TikaDocument[]::new);

			write(tikaDocuments[0], inputDocument);

			return tikaDocuments;
		}
	}

	protected UpdateResponse write(final TikaDocument tikaDocument, final SolrInputDocument inputDocument) throws
			IOException {
		final UpdateResponse response;
		boolean success = false;

		try {
			if (null != commitWithin) {
				response = client.add(inputDocument, Math.toIntExact(commitWithin.toMillis()));
			} else {
				response = client.add(inputDocument);
			}

			success = true;
		} catch (final SolrServerException e) {
			throw new TaggedIOException(new IOException(String.format("Unable to add tikaDocument to Solr: \"%s\". " +
					"There was server-side error.", tikaDocument), e), this);
		} catch (final SolrException e) {
			throw new TaggedIOException(new IOException(String.format("Unable to add tikaDocument to Solr: \"%s\". " +
					"HTTP error %d was returned.", tikaDocument, ((SolrException) e).code()), e), this);
		} catch (final IOException e) {
			throw new TaggedIOException(new IOException(String.format("Unable to add tikaDocument to Solr: \"%s\". " +
						"There was an error communicating with the server.", tikaDocument), e), this);
		} finally {
			if (!success && dump) {
				Path dumped = null;

				try {
					dumped = dump(inputDocument);
				} catch (final Exception e) {
					logger.error("Error while creating dump file.", e);
				}

				if (null != dumped) {
					logger.error("Error while adding to Solr. Input tikaDocument dumped to \"{}\".", dumped);
				}
			}
		}

		pending.incrementAndGet();

		// Autocommit if the interval is hit and enabled.
		if (commitThreshold > 0) {
			commitPending(commitThreshold);
		}

		return response;
	}

	private Path dump(final SolrInputDocument inputDocument) throws IOException {
		final Path path = Files.createTempFile("extract-dump-", ".SolrInputDocument.gz");

		try (final ObjectOutputStream out = new ObjectOutputStream(new GZIPOutputStream(Files.newOutputStream(path)))) {
			out.writeObject(inputDocument);
		}

		return path;
	}

	private void write(final EmbeddedTikaDocument child, final int level, final TikaDocument parent, final TikaDocument root)
			throws IOException {
		final SolrInputDocument inputDocument = prepareDocument(child, level, parent, root);
		final UpdateResponse response;

		// Free up memory.
		child.clearReader();

		// Null is a signal to skip.
		if (null == inputDocument) {
			return;
		}

		response = write(child, inputDocument);
		logger.info("Child document added to Solr in {}ms: \"{}\".", response.getElapsedTime(), child);

		for (EmbeddedTikaDocument grandchild : child.getEmbeds()) {
			write(grandchild, level + 1, child, root);
		}
	}

	private SolrInputDocument prepareDocument(final TikaDocument tikaDocument, final Reader reader, final int level)
			throws IOException {
		final SolrInputDocument inputDocument = new SolrInputDocument();

		// Set extracted metadata fields supplied by Tika.
		if (outputMetadata) {
			setMetadataFieldValues(tikaDocument.getMetadata(), inputDocument);
		}

		// Set tags supplied by the caller.
		tags.forEach((key, value)-> setFieldValue(inputDocument, fields.forTag(key), value));

		String id;

		try {
			id = tikaDocument.getId();
		} catch (final Exception e) {
			logger.error("Unable to get tikaDocument ID. Skipping tikaDocument.", e);
			return null;
		}

		// Set the ID. Must never be written atomically.
		if (null != fields.forId() && null != id) {
			inputDocument.setField(fields.forId(), id);
		}

		// Add the base type. De-duplicated. Eases faceting on type.
		setFieldValue(inputDocument, fields.forBaseType(), Arrays.stream(tikaDocument.getMetadata()
				.getValues(Metadata.CONTENT_TYPE)).map((type)-> {
			final MediaType mediaType = MediaType.parse(type);

			if (null == mediaType) {
				logger.warn(String.format("Content type could not be parsed: \"%s\". Was: \"%s\".", tikaDocument, type));
				return type;
			}

			return mediaType.getBaseType().toString();
		}).toArray(String[]::new));

		// Set the path field.
		if (null != fields.forPath()) {
			setFieldValue(inputDocument, fields.forPath(), tikaDocument.getPath().toString());
		}

		// Set the parent path field.
		if (null != fields.forParentPath() && tikaDocument.getPath().getNameCount() > 1) {
			setFieldValue(inputDocument, fields.forParentPath(), tikaDocument.getPath().getParent().toString());
		}

		// Set the level in the hierarchy.
		setFieldValue(inputDocument, fields.forLevel(), Integer.toString(level));

		// Finally, set the text field containing the actual extracted text.
		setFieldValue(inputDocument, fields.forText(), toString(reader));

		return inputDocument;
	}

	private SolrInputDocument prepareDocument(final EmbeddedTikaDocument child, final int level, final TikaDocument parent,
                                              final TikaDocument root) throws IOException {
		final SolrInputDocument inputDocument;

		try (final Reader reader = child.getReader()) {
			inputDocument = prepareDocument(child, reader, level);
		}

		// Null is a signal to skip the document.
		if (null == inputDocument) {
			return null;
		}

		// Set the ID of the parent on the child before adding to the parent.
		// We do this because:
		// 1) even when using child documents, Solr flattens the hierarchy (see org.apache.solr.update
		// .AddUpdateCommand#flatten);
		// 2) we need to reference the parent.
		setFieldValue(inputDocument, fields.forParentId(), parent.getId());

		// Set the ID of the root document.
		setFieldValue(inputDocument, fields.forRoot(), root.getId());

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

	private void setMetadataFieldValues(final Metadata metadata, final SolrInputDocument document) throws IOException {
		new MetadataTransformer(metadata, fields).transform((name, value)-> setFieldValue(document, name, value),
				(name, values)-> setFieldValue(document, name, values));
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
