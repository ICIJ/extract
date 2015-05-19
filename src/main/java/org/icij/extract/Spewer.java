package org.icij.extract;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import org.apache.tika.parser.ParsingReader;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class Spewer {

	public abstract void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException;
}
