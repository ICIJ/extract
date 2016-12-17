package org.icij.extract.json;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.report.Report;
import org.icij.extract.extractor.ExtractionStatus;

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
	private final DocumentFactory factory;

	public ReportDeserializer(final DocumentFactory factory, final Report report) {
		this.report = report;
		this.factory = factory;
	}

	@Override
	public Report deserialize(final JsonParser jsonParser, final DeserializationContext context) 
		throws IOException {

		jsonParser.nextToken(); // Skip over the start of the object.
		while (jsonParser.nextToken() != JsonToken.END_OBJECT && jsonParser.nextValue() != null) {
			report.put(factory.create(Paths.get(jsonParser.getCurrentName())), ExtractionStatus.parse(jsonParser
					.getValueAsInt()));
		}

		return report;
	}
}
