package org.icij.extract.core;

import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import org.apache.tika.parser.ParsingReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.TaggedOutputStream;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class StdOutSpewer extends Spewer {

	public StdOutSpewer(Logger logger) {
		super(logger);
	}

	public void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException, SpewerException {
		final TaggedOutputStream outputStream = new TaggedOutputStream(System.out);

		try {
			IOUtils.copy(reader, outputStream, outputEncoding);
		} catch (IOException e) {
			if (outputStream.isCauseOf(e)) {
				throw new SpewerException("Error writing to standard output: " + file + ".", e);
			} else {
				throw e;
			}
		}
	}
}
