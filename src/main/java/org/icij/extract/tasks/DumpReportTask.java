package org.icij.extract.tasks;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.icij.extract.report.Report;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.json.ReportSerializer;
import org.icij.extract.tasks.factories.ReportFactory;

import java.io.*;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.icij.task.MonitorableTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * A command that dumps a report to JSON output.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Dump the report for debugging. The name option is respected. If no destination path is given then the" +
		" dump is written to standard output.")
@Option(name = "report-type", description = "Set the report backend type. For now, the only valid value is" +
		" \"redis\".", parameter = "type", code = "r")
@Option(name = "report-name", description = "The name of the report, the default of which is " +
		"type-dependent.", parameter = "name")
@Option(name = "redis-address", description = "Set the Redis backend address. Defaults to " +
		"127.0.0.1:6379.", parameter = "address")
@Option(name = "report-status", description = "Only match reports with the given status.", parameter =
		"status")
public class DumpReportTask extends MonitorableTask<Void> {

	@Override
	public Void run(final String[] arguments) throws Exception {
		final Optional<ExtractionStatus> result = options.get("report-status").value(ExtractionStatus::parse);

		try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(arguments[0]));
		     final Report report = new ReportFactory(options).createShared()) {
			monitor.hintRemaining(report.size());
			dump(report, output, result.orElse(null));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("Unable to open \"%s\" for writing.", arguments[0]), e);
		}

		return null;
	}

	@Override
	public Void run() throws Exception {
		final Optional<ExtractionStatus> result = options.get("report-status").value(ExtractionStatus::parse);

		try (final OutputStream output = new BufferedOutputStream(new CloseShieldOutputStream(System.out));
		     final Report report = new ReportFactory(options).createShared()) {
			monitor.hintRemaining(report.size());
			dump(report, output, result.orElse(null));
		}

		return null;
	}

	/**
	 * Dump the report as JSON to the given output stream.
	 *
	 * @param report the report to dump
	 * @param output the stream to dump to
	 * @param match only dump matching results
	 */
	private void dump(final Report report, final OutputStream output, final ExtractionStatus match) throws
			IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addSerializer(Report.class, new ReportSerializer(monitor, match));
		mapper.registerModule(module);

		try (final JsonGenerator jsonGenerator = new JsonFactory().setCodec(mapper).createGenerator(output,
				JsonEncoding.UTF8)) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeObject(report);
			jsonGenerator.writeRaw('\n');
		}
	}
}
