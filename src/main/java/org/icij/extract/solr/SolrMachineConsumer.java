package org.icij.extract.solr;

import java.util.logging.Logger;

import java.util.function.Consumer;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrClient;

public abstract class SolrMachineConsumer implements Consumer<SolrDocument> {

	protected final Logger logger;
	protected final SolrClient client;

	protected String idField = SolrDefaults.DEFAULT_ID_FIELD;

	public SolrMachineConsumer(final Logger logger, final SolrClient client) {
		this.logger = logger;
		this.client = client;
	}

	public void setIdField(final String idField) {
		this.idField = idField;
	}

	public String getIdField() {
		return idField;
	}
}
