package org.icij.extract.tasks;

import org.icij.extract.core.Report;
import org.icij.extract.core.ExtractionResult;
import org.icij.extract.tasks.factories.ReportFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.icij.task.MonitorableTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * CLI class for replacing the the paths in a report.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Replace the given search string in paths with the replacement string.")
@Option(name = "report-type", description = "Set the report backend type. For now, the only valid value is" +
		" \"redis\".", parameter = "type", code = "r")
@Option(name = "report-name", description = "The name of the report, the default of which is " +
		"type-dependent.", parameter = "name")
@Option(name = "redis-address", description = "Set the Redis backend address. Defaults to " +
		"127.0.0.1:6379.", parameter = "address")
public class ReplaceReportTask extends MonitorableTask<Long> {

	@Override
	public Long run(final String[] literals) throws Exception {
		if (null == literals || literals.length < 1) {
			throw new IllegalArgumentException("You must pass the search string as an argument.");
		}

		try (final Report report = ReportFactory.createSharedReport(options)) {
			return replaceReport(report, literals);
		}
	}

	@Override
	public Long run() throws Exception {
		return run(null);
	}

	/**
	 * Replace paths in the given report.
	 *
	 * @param report the report to replace paths in
	 */
	private Long replaceReport(final Report report, final String[] literals) {
		final String search;
		final String replacement;
		long replaced = 0;

		search = literals[0];
		if (literals.length < 2) {
			replacement = "";
		} else {
			replacement = literals[1];
		}

		monitor.hintRemaining(report.size());

		final Pattern pattern = Pattern.compile(search);
		final Iterator<Map.Entry<Path, ExtractionResult>> iterator = report.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<Path, ExtractionResult> entry = iterator.next();
			Matcher matcher = pattern.matcher(entry.getKey().toString());

			if (matcher.matches()) {
				report.put(Paths.get(matcher.replaceFirst(replacement)), entry.getValue());
				iterator.remove();
				replaced++;
			}

			monitor.notifyListeners(entry);
		}

		return replaced;
	}
}
