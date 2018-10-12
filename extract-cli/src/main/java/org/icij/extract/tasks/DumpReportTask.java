package org.icij.extract.tasks;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.icij.extract.report.ReportMap;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.json.ReportSerializer;
import org.icij.extract.report.ReportMapFactory;

import java.io.*;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.icij.task.MonitorableTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * A command that dumps a report to JSON output.
 *
 *
 */
@Task("Dump the report for debugging. The name option is respected. If no destination path is given then the" +
		" dump is written to standard output.")
@OptionsClass(ReportMapFactory.class)
@Option(name = "reportStatus", description = "Only match reports with the given status.", parameter = "status")
public class DumpReportTask extends MonitorableTask<Void> {

	@Override
	public Void call(final String[] arguments) throws Exception {
		final Optional<ExtractionStatus> result = options.get("reportStatus").value(ExtractionStatus::parse);

		try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(arguments[0]));
		     final ReportMap reportMap = new ReportMapFactory(options).createShared()) {
			monitor.hintRemaining(reportMap.size());
			dump(reportMap, output, result.orElse(null));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("Unable to open \"%s\" for writing.", arguments[0]), e);
		}

		return null;
	}

	@Override
	public Void call() throws Exception {
		final Optional<ExtractionStatus> result = options.get("reportStatus").value(ExtractionStatus::parse);

		try (final OutputStream output = new BufferedOutputStream(new CloseShieldOutputStream(System.out));
		     final ReportMap reportMap = new ReportMapFactory(options).createShared()) {
			monitor.hintRemaining(reportMap.size());
			dump(reportMap, output, result.orElse(null));
		}

		return null;
	}

	/**
	 * Dump the report as JSON to the given output stream.
	 *
	 * @param reportMap the report to dump
	 * @param output the stream to dump to
	 * @param match only dump matching results
	 */
	private void dump(final ReportMap reportMap, final OutputStream output, final ExtractionStatus match) throws
			IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addSerializer(ReportMap.class, new ReportSerializer(monitor, match));
		mapper.registerModule(module);

		try (final JsonGenerator jsonGenerator = new JsonFactory().setCodec(mapper).createGenerator(output,
				JsonEncoding.UTF8)) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeObject(reportMap);
			jsonGenerator.writeRaw('\n');
		}
	}
}
