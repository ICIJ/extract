package org.icij.extract.json;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.report.Report;
import org.icij.extract.report.ReportMap;
import org.icij.extract.extractor.ExtractionStatus;

import java.io.IOException;

import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;

/**
 * Deserializes a {@link ReportMap} from JSON.
 *
 *
 */
public class ReportDeserializer extends JsonDeserializer<ReportMap> {

	private final ReportMap reportMap;
	private final DocumentFactory factory;

	public ReportDeserializer(final DocumentFactory factory, final ReportMap reportMap) {
		this.reportMap = reportMap;
		this.factory = factory;
	}

	@Override
	public ReportMap deserialize(final JsonParser jsonParser, final DeserializationContext context)
		throws IOException {

		jsonParser.nextToken(); // Skip over the start of the object.
		while (jsonParser.nextToken() != JsonToken.END_OBJECT && jsonParser.nextValue() != null) {
			reportMap.put(factory.create(Paths.get(jsonParser.getCurrentName())),
					new Report(ExtractionStatus.parse(jsonParser.getValueAsInt())));
		}

		return reportMap;
	}
}
