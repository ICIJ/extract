package org.icij.extract.solr;

import java.util.Map;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

/**
 * A consumer that tags the complement of documents in
 * two cores.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class SolrComplementConsumer extends SolrTaggingConsumer {

	private final SolrClient other;

	public SolrComplementConsumer(final SolrClient other,
		final SolrClient destination, final Map<String, String> literals) {
		super(destination, literals);
		this.other = other;
	}

	@Override
	protected void consume(final SolrDocument input) throws SolrServerException, IOException {
		final String id = (String) input.getFieldValue(idField);

		if (null == other.getById(id)) {
			super.consume(input);
		}
	}
}
