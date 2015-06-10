package org.icij.extract.core;

import java.util.logging.Logger;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;

import java.nio.file.FileSystems;
import java.nio.charset.StandardCharsets;

import org.apache.tika.parser.ParsingReader;
import org.apache.tika.exception.TikaException;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class PrintStreamSpewerTest {

	@Test
	public void testWrite() throws IOException, TikaException {
		final Logger logger = Logger.getLogger("extract-test");

		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final String buffer = "test";
		final ParsingReader reader = new ParsingReader(new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8)));

		spewer.write(FileSystems.getDefault().getPath("test-file"), reader, StandardCharsets.UTF_8);

		assertEquals("Output stream buffer must read \"test\"", buffer + "\n", outputStream.toString());
	}
}
