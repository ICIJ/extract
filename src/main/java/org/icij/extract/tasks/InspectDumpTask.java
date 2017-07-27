package org.icij.extract.tasks;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

@Task("Inspect a dump file.")
@Option(name = "expandChildren", description = "Also inspect child documents.")
public class InspectDumpTask extends DefaultTask<Void> {

	@Override
	public Void run() throws Exception {
		throw new IllegalArgumentException("No paths supplied.");
	}

	@Override
	public Void run(final String[] arguments) throws Exception {
		final boolean expand = options.get("expandChildren").parse().asBoolean().orElse(false);

		for (String path: arguments) {
			inspect(Paths.get(path), expand);
		}

		return null;
	}

	private void inspect(final Path path, final boolean expand) throws IOException, ClassNotFoundException {
		try (final ObjectInputStream in = new ObjectInputStream(new GZIPInputStream(Files.newInputStream(path)))) {
			final Object dump = in.readObject();

			if (dump instanceof SolrInputDocument) {
				inspect((SolrInputDocument) dump, expand);
			}
		}
	}

	private void inspect(final SolrInputDocument document, final boolean expand) {
		System.out.println("SUMMARY");
		System.out.println("-------");
		System.out.println(String.format("Child documents: %d", document.getChildDocumentCount()));
		System.out.println();

		final Iterator<SolrInputField> iterator = document.iterator();
		final List<String> warnings = new LinkedList<>();
		final int strLimit = 100;
		final int strWarningThreshold = 65535;
		final int arrLimit = 10;
		final int arrWarningThreshold = 1000;

		System.out.println("FIELDS");
		System.out.println("------");
		while (iterator.hasNext()) {
			final SolrInputField field = iterator.next();

			if (field.getValueCount() > 1) {
				final String[] values;

				values = field.getValues().stream().limit(arrLimit).map(o -> {
					String value = o.toString().trim();
					if (value.length() > strLimit) {
						value = String.format("%s... (%d other characters)", value.substring(0, strLimit),
								value.length() - strLimit);
					}

					if (value.length() > strWarningThreshold) {
						warnings.add(String.format("Field %s contains with length of %d characters.",
								field.getName(), value.length()));
					}

					return value;
				}).toArray(String[]::new);

				System.out.print(String.format("%s: %s", field.getName(), String.join(", ", values)));
				if (field.getValueCount() > arrLimit) {
					System.out.println(String.format("... (%d other items)", field.getValueCount() - arrLimit));
				} else {
					System.out.println();
				}

				if (field.getValueCount() > arrWarningThreshold) {
					warnings.add(String.format("Field %s contains array with more than %d values.",
							field.getName(), field.getValueCount()));
				}
			} else {
				String value = field.getFirstValue().toString().trim();
				if (value.length() > strLimit) {
					value = String.format("%s... (%d other characters)", value.substring(0, strLimit),
							value.length() - strLimit);
				}

				if (value.length() > strWarningThreshold) {
					warnings.add(String.format("Field %s contains with length of %d characters.", field.getName(),
							value.length()));
				}

				System.out.println(String.format("%s: %s", field.getName(), value));
			}

			if (!warnings.isEmpty()) {
				System.out.println("WARNINGS");
				System.out.println("--------");
			}

			for (String warning: warnings) {
				System.out.println(warning);
			}

			warnings.clear();
		}

		if (expand && document.getChildDocumentCount() > 0) {
			for (SolrInputDocument child: document.getChildDocuments()) {
				System.out.println();
				System.out.println();
				inspect(child, true);
			}
		}
	}
}
