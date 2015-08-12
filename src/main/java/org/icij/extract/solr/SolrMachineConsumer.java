package org.icij.extract.solr;

import java.util.logging.Logger;

import java.util.function.Consumer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrDocument;

public abstract class SolrMachineConsumer implements Consumer<SolrDocument> {

	protected final AtomicInteger consumed = new AtomicInteger();
	protected final Logger logger;

	protected String idField = SolrDefaults.DEFAULT_ID_FIELD;

	public SolrMachineConsumer(final Logger logger) {
		this.logger = logger;
	}

	public void setIdField(final String idField) {
		this.idField = idField;
	}

	public String getIdField() {
		return idField;
	}

	public int getConsumeCount() {
		return consumed.get();
	}
}
