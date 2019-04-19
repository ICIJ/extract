package org.icij.spewer;

import org.apache.commons.io.TaggedIOException;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.parser.ParsingReader;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Serializable;

/**
 * Writes the text output from a {@link ParsingReader}, and metadata, to the given {@link PrintStream}.
 *
 * @since 1.0.0-beta
 */
public class PrintStreamSpewer extends Spewer implements Serializable {

	private static final long serialVersionUID = -1952187923616552629L;
	private final PrintStream stream;

	public PrintStreamSpewer(final PrintStream stream, final FieldNames fields) {
		super(fields);
		this.stream = stream;
	}

	@Override
	public void write(final TikaDocument tikaDocument, final Reader reader) throws IOException {
		if (outputMetadata) {
			writeMetadata(tikaDocument);
		}

		// A PrintStream should never throw an IOException: the exception would always come from the input stream.
		// There's no need to use a TaggedOutputStream or catch IOExceptions.
		copy(reader, stream);

		// Add an extra newline to signify the end of the text.
		stream.println();

		if (stream.checkError()) {
			throw new TaggedIOException(new IOException("Error writing to print stream."), this);
		}

		// Write out child documents, if any.
		for (EmbeddedTikaDocument embed: tikaDocument.getEmbeds()) {
			try (final Reader embedReader = embed.getReader()) {
				write(embed, embedReader);
			}
		}
	}

	private void writeMetadata(final TikaDocument tikaDocument) throws IOException {
		final Metadata metadata = tikaDocument.getMetadata();

		// Set the path field.
		if (null != fields.forPath()) {
			stream.println(fields.forPath() + ": " + tikaDocument.getPath().toString());
		}

		// Set the parent path field.
		if (null != fields.forParentPath() && tikaDocument.getPath().getNameCount() > 1) {
			stream.println(fields.forParentPath() + ": " + tikaDocument.getPath().getParent().toString());
		}

		new MetadataTransformer(metadata, fields).transform((name, value)-> stream.println(name + ": " + value),
				(name, values)-> stream.println(name + ": " + String.join(", ", values)));

		// Add an extra newline to signify the end of the metadata.
		stream.println();
	}

	@Override
	protected void writeDocument(TikaDocument doc, Reader reader, TikaDocument parent, TikaDocument root, int level) {
		throw new UnsupportedOperationException("not implemented");
	}
}
