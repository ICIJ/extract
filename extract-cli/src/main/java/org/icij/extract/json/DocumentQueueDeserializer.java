package org.icij.extract.json;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.queue.DocumentQueue;

import java.io.IOException;

import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;

/**
 * Deserializes a {@link DocumentQueue} from JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class DocumentQueueDeserializer extends JsonDeserializer<DocumentQueue> {

	private final DocumentQueue queue;
	private final DocumentFactory factory;

	public DocumentQueueDeserializer(final DocumentFactory factory, final DocumentQueue queue) {
		this.queue = queue;
		this.factory = factory;
	}

	@Override
	public DocumentQueue deserialize(final JsonParser jsonParser, final DeserializationContext context)
		throws IOException {

		jsonParser.nextToken(); // Skip over the start of the array.
		while (jsonParser.nextToken() != JsonToken.END_ARRAY && jsonParser.nextValue() != null) {
			queue.add(factory.create(Paths.get(jsonParser.getValueAsString())));
		}

		return queue;
	}
}
