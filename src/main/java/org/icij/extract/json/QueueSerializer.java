package org.icij.extract.json;

import org.icij.extract.core.Queue;

import java.util.Iterator;

import java.io.IOException;

import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.JsonSerializer;

import hu.ssh.progressbar.ProgressBar;

/**
 * Serializes a {@link Queue} to JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class QueueSerializer extends JsonSerializer<Queue> {

	private final ProgressBar progressBar;

	public QueueSerializer(final ProgressBar progressBar) {
		this.progressBar = progressBar;
	}

	@Override
	public void serialize(final Queue queue, final JsonGenerator jsonGenerator, final SerializerProvider provider) 
		throws IOException {
		final Iterator<Path> iterator = queue.iterator();

		jsonGenerator.writeStartArray();
		while (iterator.hasNext()) {
			jsonGenerator.writeString(iterator.next().toString());

			if (null != progressBar) {
				progressBar.tickOne();
			}
		}

		jsonGenerator.writeEndArray();
	}
}
