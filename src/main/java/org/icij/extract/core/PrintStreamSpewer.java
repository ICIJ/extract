package org.icij.extract.core;

import java.util.logging.Logger;

import java.io.IOException;
import java.io.PrintStream;

import java.nio.file.Path;
import java.nio.charset.Charset;

import org.apache.tika.parser.ParsingReader;

import org.apache.commons.io.IOUtils;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class PrintStreamSpewer extends Spewer {

	private final PrintStream printStream;

	public PrintStreamSpewer(Logger logger, PrintStream printStream) {
		super(logger);
		this.printStream = printStream;
	}

	public void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException, SpewerException {

		// A PrintStream should never throw an IOException: the exception would always come from the input stream.
		// There's no need to use a TaggedOutputStream or catch IOExceptions.
		IOUtils.copy(reader, printStream, outputEncoding);

		if (printStream.checkError()) {
			throw new SpewerException("Error writing to print stream: " + file + ".");
		}
	}
}
