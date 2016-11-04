package org.icij.extract.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;

import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.TikaException;

import org.junit.Test;
import org.junit.Assert;

public class PrintStreamSpewerTest {

	@Test
	public void testWrite() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream);

		final String buffer = "$";
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(Paths.get(name), new Metadata(), reader);

		Assert.assertEquals("$\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, outputStream.toByteArray());
	}

	@Test
	public void testWriteFromUTF16LE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream);

		final byte[] buffer = new byte[] {(byte) 0xFF, (byte) 0xFE, 0x24, 0x00};
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer);
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(Paths.get(name), new Metadata(), reader);

		Assert.assertEquals("$\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, outputStream.toByteArray());
	}

	@Test
	public void testWriteFromUTF16BE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream);

		final byte[] buffer = new byte[] {(byte) 0xFE, (byte) 0xFF, 0x00, 0x24};
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer);
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.write(Paths.get(name), new Metadata(), reader);

		Assert.assertEquals("$\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, outputStream.toByteArray());
	}

	@Test
	public void testWriteToUTF16LE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream);

		// Declare file contents of a single dollar sign ($).
		final String buffer = "\u0024";
		final String name = "imaginary-file.txt";

		// Tika parsers always output UTF-8.
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.setOutputEncoding(StandardCharsets.UTF_16LE);
		spewer.write(Paths.get("test-file"), new Metadata(), reader);

		Assert.assertArrayEquals(new byte[] {0x24, 0x00, 0x0A, 0x00}, outputStream.toByteArray());
	}

	@Test
	public void testWriteToUTF16BE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream);

		// Declare file contents of a single dollar sign ($).
		final String buffer = "\u0024";
		final String name = "imaginary-file.txt";

		// Tika parsers always output UTF-8.
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new TextParsingReader(inputStream, name);

		spewer.setOutputEncoding(StandardCharsets.UTF_16BE);
		spewer.write(Paths.get("test-file"), new Metadata(), reader);

		Assert.assertArrayEquals(new byte[] {0x00, 0x24, 0x00, 0x0A}, outputStream.toByteArray());
	}
}
