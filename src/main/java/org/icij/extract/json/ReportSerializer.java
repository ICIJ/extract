package org.icij.extract.json;

import org.icij.extract.core.Report;
import org.icij.extract.core.ReportResult;

import java.util.Iterator;
import java.util.Map;

import java.io.IOException;

import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.JsonSerializer;

import hu.ssh.progressbar.ProgressBar;

/**
 * Serializes a {@link Report} to JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ReportSerializer extends JsonSerializer<Report> {

	private final ProgressBar progressBar;
	private final ReportResult match;

	public ReportSerializer(final ProgressBar progressBar, final ReportResult match) {
		this.progressBar = progressBar;
		this.match = match;
	}

	@Override
	public void serialize(final Report report, final JsonGenerator jsonGenerator, final SerializerProvider provider) 
		throws IOException, JsonProcessingException {
		final Iterator<Map.Entry<Path, ReportResult>> iterator = report.entrySet().iterator();

		jsonGenerator.writeStartObject();
		while (iterator.hasNext()) {
			Map.Entry<Path, ReportResult> entry = (Map.Entry<Path, ReportResult>) iterator.next();
			ReportResult result = entry.getValue();

			if (null == match || result.equals(match)) {
				jsonGenerator.writeObjectField(entry.getKey().toString(), result.getValue());
			}

			if (null != progressBar) {
				progressBar.tickOne();
			}
		}

		jsonGenerator.writeEndObject();
	}
}
