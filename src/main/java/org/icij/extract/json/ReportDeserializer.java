package org.icij.extract.json;

import org.icij.extract.core.Report;
import org.icij.extract.core.ReportResult;

import java.io.IOException;

import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;

/**
 * Deserializes a {@link Report} from JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ReportDeserializer extends JsonDeserializer<Report> {

	private final Report report;

	public ReportDeserializer(final Report report) {
		this.report = report;
	}

	@Override
	public Report deserialize(final JsonParser jsonParser, final DeserializationContext context) 
		throws IOException {

		jsonParser.nextToken(); // Skip over the start of the object.
		while (jsonParser.nextToken() != JsonToken.END_OBJECT && jsonParser.nextValue() != null) {
			report.put(Paths.get(jsonParser.getCurrentName()), ReportResult.get(jsonParser.getValueAsInt()));
		}

		return report;
	}
}
