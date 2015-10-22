package org.icij.extract.solr;

import java.util.logging.Logger;

import java.util.function.Consumer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrDocument;

import hu.ssh.progressbar.ProgressBar;

public abstract class SolrMachineConsumer implements Consumer<SolrDocument> {

	protected final AtomicInteger consumed = new AtomicInteger();
	protected final Logger logger;

	protected String idField = SolrDefaults.DEFAULT_ID_FIELD;
	protected ProgressBar progressBar = null;

	public SolrMachineConsumer(final Logger logger) {
		this.logger = logger;
	}

	@Override
	public void accept(final SolrDocument input) {
		try {
			consume(input);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (null != progressBar) {
				progressBar.tickOne();
			}
		}

		consumed.incrementAndGet();
	}

	protected abstract void consume(final SolrDocument input) throws Exception;

	public void setIdField(final String idField) {
		this.idField = idField;
	}

	public String getIdField() {
		return idField;
	}

	public int getConsumeCount() {
		return consumed.get();
	}

	public void setProgressBar(final ProgressBar progressBar) {
		this.progressBar = progressBar;
	}

	public ProgressBar getProgressBar() {
		return progressBar;
	}
}
