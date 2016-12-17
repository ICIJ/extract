package org.icij.extract.spewer;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.icij.extract.document.Document;
import org.icij.task.Options;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link SolrSpewer} that merges the {@literal path} and {@literal parent path} fields from the given
 * {@link Document} with those from any document with the same ID on the Solr server.
 *
 * This functionality allows documents with file-digest-type IDs to hold multiple paths, reflecting the multiple
 * duplicate copies that may exist on disk.
 */
public class MergingSolrSpewer extends SolrSpewer {

	private int retries = 30;

	public MergingSolrSpewer(final SolrClient client, final FieldNames fields) {
		super(client, fields);
	}

	public MergingSolrSpewer configure(final Options<String> options) {
		super.configure(options);
		options.get("retries-on-conflict").parse().asInteger().ifPresent(this::setRetries);

		return this;
	}

	public void setRetries(final int retries) {
		this.retries = retries;
	}

	@Override
	protected UpdateResponse write(final Document document, final SolrInputDocument inputDocument) throws
			IOException, SolrServerException {
		return write(document, inputDocument, retries);
	}

	private UpdateResponse write(final Document document, final SolrInputDocument inputDocument, int retries) throws
			IOException, SolrServerException {
		merge(document, inputDocument);

		try {
			return super.write(document, inputDocument);
		} catch (SolrException e) {
			if (retries > 0 && e.code() == 409) {
				return write(document, inputDocument, --retries);
			}

			throw e;
		}
	}

	private void merge(final Document document, final SolrInputDocument inputDocument) throws IOException,
			SolrServerException {
		final SolrDocument existingDocument;

		// Request only the fields which must be merged, not the entire document.
		final ModifiableSolrParams params = new ModifiableSolrParams();
		params.set(CommonParams.FL, fields.forPath(), fields.forParentPath(), fields.forVersion());
		existingDocument = client.getById(document.getId(), params);

		// Since we're updating the path and parent path values of an existing document, set the version field to
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
			mergeField(fields.forPath(), document.getPath().toString(), existingDocument, inputDocument);
		}

		// Set the parent path field.
		if (null != fields.forParentPath() && document.getPath().getNameCount() > 1) {
			mergeField(fields.forParentPath(), document.getPath().getParent().toString(), existingDocument,
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
