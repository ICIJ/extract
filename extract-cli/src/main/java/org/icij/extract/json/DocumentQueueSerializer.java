package org.icij.extract.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.icij.event.Notifiable;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.queue.DocumentQueue;

import java.io.IOException;
import java.util.Iterator;

/**
 * Serializes a {@link DocumentQueue} to JSON.
 */
public class DocumentQueueSerializer extends JsonSerializer<DocumentQueue> {

	private final Notifiable monitor;

	public DocumentQueueSerializer(final Notifiable monitor) {
		this.monitor = monitor;
	}

	@Override
	public void serialize(final DocumentQueue queue, final JsonGenerator jsonGenerator, final SerializerProvider provider)
		throws IOException {
		final Iterator<TikaDocument> iterator = queue.iterator();

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
