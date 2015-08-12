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

public class SolrTaggingConsumer extends SolrMachineConsumer {

	private final Map<String, String> literals;
	private final SolrClient destination;

	public SolrTaggingConsumer(final Logger logger, final SolrClient destination,
		final Map<String, String> literals) {
		super(logger);
		this.destination = destination;
		this.literals = literals;
	}

	@Override
	public void accept(final SolrDocument input) {
		try {
			tag(input);
		} catch (SolrServerException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected void tag(final SolrDocument input) throws SolrServerException, IOException {
		final SolrInputDocument output = new SolrInputDocument(); 
		final String id = (String) input.getFieldValue(idField);

		output.setField(idField, id);
		for (Map.Entry<String, String> entry : literals.entrySet()) {
			Map<String, Object> atomic = new HashMap<String, Object>();

			atomic.put("set", entry.getValue());
			output.setField(entry.getKey(), atomic);
		}

		logger.info(String.format("Tagging document with ID %s.", id));
		destination.add(output);
		consumed.incrementAndGet();
	}
}
