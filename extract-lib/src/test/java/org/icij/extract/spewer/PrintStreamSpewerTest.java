package org.icij.extract.spewer;

import org.apache.tika.exception.TikaException;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.parser.ParsingReader;
import org.icij.spewer.FieldNames;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class PrintStreamSpewerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	@Test
	public void testWrite() throws IOException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream, new FieldNames());

		final String buffer = "$";
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new ParsingReader(inputStream, name);
		spewer.outputMetadata(false);
		TikaDocument document = factory.create(name);
		document.setReader(reader);
		spewer.write(document);

		Assert.assertEquals("$\n\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, Arrays.copyOfRange(outputStream.toByteArray(), 0, 2));
	}

	@Test
	public void testWriteFromUTF16LE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream, new FieldNames());

		final byte[] buffer = new byte[] {(byte) 0xFF, (byte) 0xFE, 0x24, 0x00};
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer);
		final ParsingReader reader = new ParsingReader(inputStream, name);

		spewer.outputMetadata(false);
		TikaDocument document = factory.create(name);
		document.setReader(reader);
		spewer.write(document);

		Assert.assertEquals("$\n\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, Arrays.copyOfRange(outputStream.toByteArray(), 0, 2));
	}

	@Test
	public void testWriteFromUTF16BE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream, new FieldNames());

		final byte[] buffer = new byte[] {(byte) 0xFE, (byte) 0xFF, 0x00, 0x24};
		final String name = "imaginary-file.txt";
		final InputStream inputStream = new ByteArrayInputStream(buffer);
		final ParsingReader reader = new ParsingReader(inputStream, name);

		spewer.outputMetadata(false);
		TikaDocument document = factory.create(name);
		document.setReader(reader);
		spewer.write(document);

		Assert.assertEquals("$\n\n", outputStream.toString(StandardCharsets.UTF_8.name()));
		Assert.assertArrayEquals(new byte[] {0x24, 0x0A}, Arrays.copyOfRange(outputStream.toByteArray(), 0, 2));
	}

	@Test
	public void testWriteToUTF16LE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream, new FieldNames());

		// Declare file contents of a single dollar sign ($).
		final String buffer = "\u0024";
		final String name = "imaginary-file.txt";

		// Tika parsers always output UTF-8.
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new ParsingReader(inputStream, name);

		spewer.outputMetadata(false);
		spewer.setOutputEncoding(StandardCharsets.UTF_16LE);
		TikaDocument document = factory.create("test-file");
		document.setReader(reader);
		spewer.write(document);

		Assert.assertArrayEquals(new byte[] {0x24, 0x00, 0x0A, 0x00}, Arrays.copyOfRange(outputStream.toByteArray(),
				0, 4));
	}

	@Test
	public void testWriteToUTF16BE() throws IOException, TikaException {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(printStream, new FieldNames());

		// Declare file contents of a single dollar sign ($).
		final String buffer = "\u0024";
		final String name = "imaginary-file.txt";

		// Tika parsers always output UTF-8.
		final InputStream inputStream = new ByteArrayInputStream(buffer.getBytes(StandardCharsets.UTF_8));
		final ParsingReader reader = new ParsingReader(inputStream, name);

		spewer.outputMetadata(false);
		spewer.setOutputEncoding(StandardCharsets.UTF_16BE);
		TikaDocument document = factory.create("test-file");
		document.setReader(reader);
		spewer.write(document);

		Assert.assertArrayEquals(new byte[] {0x00, 0x24, 0x00, 0x0A}, Arrays.copyOfRange(outputStream.toByteArray(),
				0, 4));
	}
}
