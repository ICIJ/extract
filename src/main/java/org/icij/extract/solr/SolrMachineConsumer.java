package org.icij.extract.solr;

import java.util.function.Consumer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrDocument;

import org.icij.events.Notifiable;
import org.icij.extract.spewer.FieldNames;

public abstract class SolrMachineConsumer implements Consumer<SolrDocument> {

	private final AtomicInteger consumed = new AtomicInteger();

	String idField = FieldNames.DEFAULT_ID_FIELD;
	private Notifiable notifiable = null;

	@Override
	public void accept(final SolrDocument input) {
		try {
			consume(input);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (null != notifiable) {
				notifiable.notifyListeners(input);
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

	public void setNotifiable(final Notifiable notifiable) {
		this.notifiable = notifiable;
	}

	public Notifiable getNotifiable() {
		return notifiable;
	}
}
