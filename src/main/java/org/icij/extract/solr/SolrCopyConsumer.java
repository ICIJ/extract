package org.icij.extract.solr;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;


public class SolrCopyConsumer extends SolrMachineConsumer {

	private final Map<String, String> map;

	public SolrCopyConsumer(final Logger logger, final SolrClient client, final Map<String, String> map) {
		super(logger, client);
		this.map = map;
	}

	@Override
	public void accept(final SolrDocument input) {
		try {
			copy(input);
		} catch (SolrServerException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void copyField(final String from, final SolrDocument input,
		final SolrInputDocument output) {
		final Map<String, Object> atomic = new HashMap<String, Object>();
		String to = map.get(from);

		// If there's no target field, copy the field onto itself.
		// This forces reindexing in Solr.
		if (null == to) {
			to = from;
		}

		// The ID field can't be set atomically.
		if (to.equals(idField)) {
			output.setField(to, input.getFieldValue(idField));
		} else {
			atomic.put("set", input.getFieldValue(from));
			output.setField(to, atomic);
		}
	}

	private void copy(final SolrDocument input) throws SolrServerException, IOException {
		final SolrInputDocument output = new SolrInputDocument(); 

		// Copy the source fields to the target fields.
		// Copy all the fields from the returned document. This ensures that
		// wildcard matches work.
		for (String field : input.keySet()) {
			copyField(field, input, output);
		}

		logger.info(String.format("Adding document with ID %s.",
			input.getFieldValue(idField)));
		client.add(output);
		consumed.incrementAndGet();
	}
}
