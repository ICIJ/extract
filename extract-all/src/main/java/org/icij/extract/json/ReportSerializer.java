package org.icij.extract.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.icij.event.Notifiable;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.Report;
import org.icij.extract.report.ReportMap;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Serializes a {@link ReportMap} to JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ReportSerializer extends JsonSerializer<ReportMap> {

	private final Notifiable monitor;
	private final ExtractionStatus match;

	public ReportSerializer(final Notifiable monitor, final ExtractionStatus match) {
		this.monitor = monitor;
		this.match = match;
	}

	@Override
	public void serialize(final ReportMap reportMap, final JsonGenerator jsonGenerator, final SerializerProvider provider)
		throws IOException {
		final Iterator<Map.Entry<TikaDocument, Report>> iterator = reportMap.entrySet().iterator();

		jsonGenerator.writeStartObject();
		while (iterator.hasNext()) {
			final Map.Entry<TikaDocument, Report> entry = iterator.next();
			final ExtractionStatus result = entry.getValue().getStatus();

			if (null == match || result.equals(match)) {
				jsonGenerator.writeObjectField(entry.getKey().toString(), result.getCode());
			}

			if (null != monitor) {
				monitor.notifyListeners();
			}
		}

		jsonGenerator.writeEndObject();
	}
}
