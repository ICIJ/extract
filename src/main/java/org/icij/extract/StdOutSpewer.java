package org.icij.extract;

import java.util.logging.Logger;

import java.io.IOException;

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
public class StdOutSpewer extends Spewer {

	public StdOutSpewer(Logger logger) {
		super(logger);
	}

	public void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException {
		IOUtils.copy(reader, System.out, outputEncoding);
	}
}
