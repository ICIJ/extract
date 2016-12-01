package org.icij.extract.json;

import org.icij.extract.queue.PathQueue;

import java.io.IOException;

import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;

/**
 * Deserializes a {@link PathQueue} from JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class PathQueueDeserializer extends JsonDeserializer<PathQueue> {

	private final PathQueue queue;

	public PathQueueDeserializer(final PathQueue queue) {
		this.queue = queue;
	}

	@Override
	public PathQueue deserialize(final JsonParser jsonParser, final DeserializationContext context)
		throws IOException {

		jsonParser.nextToken(); // Skip over the start of the array.
		while (jsonParser.nextToken() != JsonToken.END_ARRAY && jsonParser.nextValue() != null) {
			queue.add(Paths.get(jsonParser.getValueAsString()));
		}

		return queue;
	}
}
