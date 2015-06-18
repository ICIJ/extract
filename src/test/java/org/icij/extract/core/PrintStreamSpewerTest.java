package org.icij.extract.core;

import java.util.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;

import java.nio.file.FileSystems;
import java.nio.charset.StandardCharsets;

import org.apache.tika.exception.TikaException;

import org.junit.Test;
import org.junit.Assert;

public class PrintStreamSpewerTest {

	private final Logger logger = Logger.getLogger("extract-test");

	@Test
	public void testWrite() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final String buffer = "$";
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(FileSystems.getDefault().getPath(name), reader, StandardCharsets.UTF_8);

		Assert.assertEquals("$\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, outputStream.toByteArray());
	}

	@Test
	public void testWriteFromUTF16LE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final byte[] buffer = new byte[] {(byte) 0xFF, (byte) 0xFE, 0x24, 0x00};
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer);
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(FileSystems.getDefault().getPath(name), reader, StandardCharsets.UTF_8);

		Assert.assertEquals("$\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, outputStream.toByteArray());
	}

	@Test
	public void testWriteFromUTF16BE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final byte[] buffer = new byte[] {(byte) 0xFE, (byte) 0xFF, 0x00, 0x24};
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer);
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(FileSystems.getDefault().getPath(name), reader, StandardCharsets.UTF_8);

		Assert.assertEquals("$\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, outputStream.toByteArray());
	}

	@Test
	public void testWriteToUTF16LE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		// Declare file contents of a single dollar sign ($).
		final String buffer = "\u0024";
		final String name = "imaginary-file.txt";

		// Tika parsers always output UTF-8.
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(FileSystems.getDefault().getPath("test-file"), reader, StandardCharsets.UTF_16LE);

		Assert.assertArrayEquals(new byte[] {0x24, 0x00, 0x0A, 0x00}, outputStream.toByteArray());
	}

	@Test
	public void testWriteToUTF16BE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		// Declare file contents of a single dollar sign ($).
		final String buffer = "\u0024";
		final String name = "imaginary-file.txt";

		// Tika parsers always output UTF-8.
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(FileSystems.getDefault().getPath("test-file"), reader, StandardCharsets.UTF_16BE);

		Assert.assertArrayEquals(new byte[] {0x00, 0x24, 0x00, 0x0A}, outputStream.toByteArray());
	}
}
