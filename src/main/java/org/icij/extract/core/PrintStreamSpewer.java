package org.icij.extract.core;

import java.io.Reader;
import java.io.IOException;
import java.io.PrintStream;

import java.nio.file.Path;

import org.apache.tika.metadata.Metadata;

import org.apache.commons.io.IOUtils;
import org.icij.task.Options;

/**
 * Writes the text output from a {@link ParsingReader} to the given {@link PrintStream}.
 *
 * @since 1.0.0-beta
 */
public class PrintStreamSpewer extends Spewer {

	private final PrintStream stream;

	public PrintStreamSpewer(final PrintStream stream) {
		this.stream = stream;
	}

	public PrintStreamSpewer(final PrintStream stream, final Options<String> options) {
		super(options);
		this.stream = stream;
	}

	@Override
	public void write(final Path file, final Metadata metadata, final Reader reader) throws IOException {

		// A PrintStream should never throw an IOException: the exception would always come from the input stream.
		// There's no need to use a TaggedOutputStream or catch IOExceptions.
		IOUtils.copy(reader, stream, outputEncoding);

		if (stream.checkError()) {
			throw new SpewerException(String.format("Error writing to print stream: \"%s\".", file));
		}
	}

	@Override
	public void close() throws IOException {
		if (!stream.equals(System.out) && !stream.equals(System.err)) {
			stream.close();
		}
	}
}
