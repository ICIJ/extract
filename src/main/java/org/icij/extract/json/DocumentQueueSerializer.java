package org.icij.extract.json;

import org.icij.extract.document.Document;
import org.icij.extract.queue.DocumentQueue;

import java.util.Iterator;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.JsonSerializer;

import org.icij.events.Notifiable;

/**
 * Serializes a {@link DocumentQueue} to JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class DocumentQueueSerializer extends JsonSerializer<DocumentQueue> {

	private final Notifiable monitor;

	public DocumentQueueSerializer(final Notifiable monitor) {
		this.monitor = monitor;
	}

	@Override
	public void serialize(final DocumentQueue queue, final JsonGenerator jsonGenerator, final SerializerProvider provider)
		throws IOException {
		final Iterator<Document> iterator = queue.iterator();

		jsonGenerator.writeStartArray();
		while (iterator.hasNext()) {
			jsonGenerator.writeString(iterator.next().getPath().toString());

			if (null != monitor) {
				monitor.notifyListeners();
			}
		}

		jsonGenerator.writeEndArray();
	}
}
