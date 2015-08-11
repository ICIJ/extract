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

public class SolrIntersectionConsumer extends SolrMachineConsumer {

	private final Map<String, String> literals;
	private final SolrClient b;
	private final SolrClient c;

	public SolrIntersectionConsumer(final Logger logger, final SolrClient a, final SolrClient b,
		final SolrClient c, final Map<String, String> literals) {
		super(logger, a);
		this.b = b;
		this.c = c;
		this.literals = literals;
	}

	@Override
	public void accept(final SolrDocument input) {
		try {
			intersect(input);
		} catch (SolrServerException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void intersect(final SolrDocument input) throws SolrServerException, IOException {
		final String id = (String) input.getFieldValue(idField);

		if (null != b.getById(id)) {
			tagIntersect(id);
		}
	}

	private void tagIntersect(final String id) throws SolrServerException, IOException {
		final SolrInputDocument output = new SolrInputDocument(); 

		output.setField(idField, id);
		for (Map.Entry<String, String> entry : literals.entrySet()) {
			Map<String, Object> atomic = new HashMap<String, Object>();

			atomic.put("set", entry.getValue());
			output.setField(entry.getKey(), atomic);
		}

		logger.info(String.format("Tagging document with ID %s.", id));
		c.add(output);
		consumed.incrementAndGet();
	}
}
