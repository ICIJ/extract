package org.icij.extract.spewer;

import org.icij.extract.spewer.SolrSpewer;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link SolrSpewer} that merges the {@literal path} and {@literal parent path} fields from the given
 * {@link TikaDocument} with those from any document with the same ID on the Solr server.
 *
 * This functionality allows documents with file-digest-type IDs to hold multiple paths, reflecting the multiple
 * duplicate copies that may exist on disk.
 */
@Option(name = "retriesOnConflict", description = "The number of times to retry adding a document when a " +
		"conflict error is returned by the index, after merging in existing fields.", parameter = "number")
@Option(name = "anotheroption", description = "yet more option " +
		"continues", parameter="number")
public class MergingSolrSpewer extends SolrSpewer {

	private static final long serialVersionUID = -7084864083664544361L;
	private int retries = 100;

	public MergingSolrSpewer(final SolrClient client, final FieldNames fields) {
		super(client, fields);

	}

	public MergingSolrSpewer configure(final Options<String> options) {
		super.configure(options);
//                System.out.println(options);
                //REMOVED AS TRIGGERED NULLPOINTEXCEPTION; TODO: FIND CAUSE
                //options.get("retriesOnConflict").parse().asInteger().ifPresent(this::setRetries);
                this.retries = retries; //temp fix
		return this;
	}
        

	public void setRetries(final int retries) {
		this.retries = retries;
	}

	@Override
	protected UpdateResponse write(final TikaDocument tikaDocument, final SolrInputDocument inputDocument) throws
			IOException {

		// Only root documents are merged.
		if (tikaDocument instanceof EmbeddedTikaDocument) {
			return super.write(tikaDocument, inputDocument);
		} else {
			return write(tikaDocument, inputDocument, retries);
		}
	}

	private UpdateResponse write(final TikaDocument tikaDocument, final SolrInputDocument inputDocument, final int retries)
			throws IOException {
		try {
			merge(tikaDocument, inputDocument);
		} catch (final SolrServerException e) {
			throw new IOException(e);
		}

		try {
			return super.write(tikaDocument, inputDocument);
		} catch (SolrException e) {
			if (retries > 0 && e.code() == 409) {
				return write(tikaDocument, inputDocument, retries - 1);
			}

			throw e;
		}
	}

	private void merge(final TikaDocument tikaDocument, final SolrInputDocument inputDocument) throws IOException,
			SolrServerException {
		final SolrDocument existingDocument;
		final SolrQuery params = new SolrQuery();
		final String resourceNameKey = fields.forMetadata(Metadata.RESOURCE_NAME_KEY);

		// The tikaDocument must be retrieved from the real-time-get (RTG) handler, otherwise we'd have to commit every
		// time a tikaDocument is added.
		params.setRequestHandler("/get");

		// Request only the fields which must be merged, not the entire tikaDocument.
		params.setFields(fields.forPath(), fields.forParentPath(), fields.forVersion(), resourceNameKey);
		existingDocument = client.getById(tikaDocument.getId(), params);

		// Since we're updating the path and parent path values of an existing tikaDocument, set the version field to
		// avoid conflicts. Note that child documents don't have a version field.
		if (null != existingDocument) {
			final Object version = existingDocument.getFieldValue(fields.forVersion());
			if (null != version) {
				inputDocument.setField(fields.forVersion(), version);
			}

		} else {
			inputDocument.setField(fields.forVersion(), "-1");
		}

		// Set the path field.
		if (null != fields.forPath()) {
			mergeField(fields.forPath(), tikaDocument.getPath().toString(), existingDocument, inputDocument);
		}

		// Set the parent path field.
		if (null != fields.forParentPath() && tikaDocument.getPath().getNameCount() > 1) {
			mergeField(fields.forParentPath(), tikaDocument.getPath().getParent().toString(), existingDocument,
					inputDocument);
		}

		// Merge the resource name field.
		if (tikaDocument.getMetadata() != null) {
			mergeField(resourceNameKey, tikaDocument.getMetadata().get(Metadata.RESOURCE_NAME_KEY), existingDocument,
					inputDocument);
		}
	}

	private void mergeField(final String name, final String newValue, final SolrDocument existingDocument, final
	SolrInputDocument inputDocument) {

		// Even though the superclass sets the path and parent path fields, we should set them again in case there's
		// a retry and they need to be overwritten.
		if (null == existingDocument) {
			setFieldValue(inputDocument, name, newValue);
			return;
		}

		// Create a HashSet from existing values so that only non-existing (distinct) values are added.
		// A HashSet gives constant time performance, as opposed to a loop, which is important when dealing with
		// potentially thousands of values.
		final Collection<Object> existingValues = existingDocument.getFieldValues(name);
		if (null != existingValues) {
			final Set<String> values = existingValues.stream()
					.map(String::valueOf)
					.collect(Collectors.toCollection(HashSet::new));

			values.add(newValue);
			setFieldValue(inputDocument, name, values.toArray(new String[values.size()]));
		} else {
			setFieldValue(inputDocument, name, newValue);
		}
	}
}
