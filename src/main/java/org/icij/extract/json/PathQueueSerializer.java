package org.icij.extract.json;

import org.icij.extract.queue.PathQueue;

import java.util.Iterator;

import java.io.IOException;

import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.JsonSerializer;

import org.icij.events.Notifiable;

/**
 * Serializes a {@link PathQueue} to JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class PathQueueSerializer extends JsonSerializer<PathQueue> {

	private final Notifiable monitor;

	public PathQueueSerializer(final Notifiable monitor) {
		this.monitor = monitor;
	}

	@Override
	public void serialize(final PathQueue queue, final JsonGenerator jsonGenerator, final SerializerProvider provider)
		throws IOException {
		final Iterator<Path> iterator = queue.iterator();

		jsonGenerator.writeStartArray();
		while (iterator.hasNext()) {
			jsonGenerator.writeString(iterator.next().toString());

			if (null != monitor) {
				monitor.notifyListeners();
			}
		}

		jsonGenerator.writeEndArray();
	}
}
