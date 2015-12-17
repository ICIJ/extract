package org.icij.extract.json;

import org.icij.extract.core.Queue;

import java.io.IOException;

import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;

import hu.ssh.progressbar.ProgressBar;

/**
 * Deserializes a {@link Queue} from JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class QueueDeserializer extends JsonDeserializer<Queue> {

	private final Queue queue;

	public QueueDeserializer(final Queue queue) {
		this.queue = queue;
	}

	@Override
	public Queue deserialize(final JsonParser jsonParser, final DeserializationContext context) 
		throws IOException, JsonProcessingException {

		jsonParser.nextToken(); // Skip over the start of the object.
		while (jsonParser.nextToken() != JsonToken.END_OBJECT && jsonParser.nextValue() != null) {
			queue.add(Paths.get(jsonParser.getValueAsString()));
		}

		return queue;
	}
}
