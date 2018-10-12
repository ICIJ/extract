package org.icij.extract.tasks;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.report.ReportMap;
import org.icij.extract.json.ReportDeserializer;
import org.icij.extract.report.ReportMapFactory;

import java.io.*;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * CLI class for loading a report from JSON.
 *
 *
 */
@Task("Load a report from a JSON dump file. The name option is respected. If no source path is given then " +
		"the input is read from standard input.")
@OptionsClass(ReportMapFactory.class)
public class LoadReportTask extends DefaultTask<Void> {

	@Override
	public Void call() throws Exception {
		final DocumentFactory factory = new DocumentFactory().configure(options);

		try (final InputStream input = new CloseShieldInputStream(System.in);
		     final ReportMap reportMap = new ReportMapFactory(options)
				     .withDocumentFactory(factory)
				     .createShared()) {
			load(factory, reportMap, input);
		}

		return null;
	}

	@Override
	public Void call(final String[] arguments) throws Exception {
		final DocumentFactory factory = new DocumentFactory().configure(options);

		try (final ReportMap reportMap = new ReportMapFactory(options)
				.withDocumentFactory(factory)
				.createShared()) {
			for (String argument : arguments) {
				load(factory, reportMap, argument);
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Unable to open dump file for reading.", e);
		}

		return null;
	}

	/**
	 * Load a dump file from the given path into a report.
	 *
	 * @param reportMap the queue to load the dump into
	 * @param path the path to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void load(final DocumentFactory factory, final ReportMap reportMap, final String path) throws IOException {
		try (final InputStream input = new BufferedInputStream(new FileInputStream(path))) {
			load(factory, reportMap, input);
		}
	}

	/**
	 * Load dump JSON from the given stream into a report.
	 *
	 * @param reportMap the report to load into
	 * @param input the input stream to load from
	 */
	private void load(final DocumentFactory factory, final ReportMap reportMap, final InputStream input) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addDeserializer(ReportMap.class, new ReportDeserializer(factory, reportMap));
		mapper.registerModule(module);

		try (final JsonParser jsonParser = new JsonFactory().setCodec(mapper).createParser(input)) {
			jsonParser.readValueAs(ReportMap.class);
		}
	}
}
