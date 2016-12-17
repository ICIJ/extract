package org.icij.extract.spewer;

import java.io.Reader;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.tika.metadata.Metadata;

import org.apache.commons.io.IOUtils;
import org.icij.extract.document.Document;
import org.icij.extract.document.EmbeddedDocument;
import org.icij.extract.parser.ParsingReader;

/**
 * Writes the text output from a {@link ParsingReader}, and metadata, to the given {@link PrintStream}.
 *
 * @since 1.0.0-beta
 */
public class PrintStreamSpewer extends Spewer {

	private final PrintStream stream;

	public PrintStreamSpewer(final PrintStream stream, final FieldNames fields) {
		super(fields);
		this.stream = stream;
	}

	@Override
	public void write(final Document document, final Reader reader) throws IOException {
		if (outputMetadata) {
			writeMetadata(document);
		}

		// A PrintStream should never throw an IOException: the exception would always come from the input stream.
		// There's no need to use a TaggedOutputStream or catch IOExceptions.
		IOUtils.copy(reader, stream, outputEncoding);

		// Add an extra newline to signify the end of the text.
		stream.println();

		if (stream.checkError()) {
			throw new SpewerException(String.format("Error writing to print stream: \"%s\".", document));
		}

		// Write out child documents, if any.
		for (EmbeddedDocument embed: document.getEmbeds()) {
			Reader embedReader = embed.getReader();

			stream.println("### ATTACHMENT ###");
			write(embed, embedReader);
			embedReader.close();
		}
	}

	@Override
	public void writeMetadata(final Document document) throws IOException {
		final Metadata metadata = document.getMetadata();

		// Set the path field.
		if (null != fields.forPath()) {
			stream.println(String.format("%s: %s", fields.forPath(), document.getPath().toString()));
		}

		// Set the parent path field.
		if (null != fields.forParentPath() && document.getPath().getNameCount() > 1) {
			stream.println(String.format("%s: %s", fields.forParentPath(), document.getPath().getParent().toString()));
		}

		for (String name : metadata.names()) {
			stream.println(String.format("%s: %s", normalizeMetadataName(name),
					String.join(", ", metadata.getValues(name)
			)));
		}

		// Add an extra newline to signify the end of the metadata.
		stream.println();
	}

	@Override
	public void close() throws IOException {
		if (!stream.equals(System.out) && !stream.equals(System.err)) {
			stream.close();
		}
	}
}
