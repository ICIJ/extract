package org.icij.extract.spewer;

import org.apache.james.mime4j.dom.field.FieldName;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class SpewerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	private class SpewerStub extends Spewer {

		final Map<String, String> metadata = new HashMap<>();

		SpewerStub() {
			super(new FieldNames());
		}

		@Override
		public void write(final Document document, final Reader reader) throws IOException {
		}

		@Override
		public void writeMetadata(final Document document) throws IOException {
			final Metadata metadata = document.getMetadata();

			applyMetadata(metadata, this.metadata::put,
					(name, values)-> Stream.of(values).forEach(value -> this.metadata.put(name, value)));
		}

		@Override
		public void close() throws IOException {
			metadata.clear();
		}
	}

	@Test
	public void testDefaultOutputEncodingIsUTF8() {
		Assert.assertEquals(StandardCharsets.UTF_8, new SpewerStub().getOutputEncoding());
	}

	@Test
	public void testSetOutputEncoding() {
		final Spewer spewer = new SpewerStub();

		spewer.setOutputEncoding(StandardCharsets.US_ASCII);
		Assert.assertEquals(StandardCharsets.US_ASCII, spewer.getOutputEncoding());
	}

	@Test
	public void testDefaultIsToOutputMetadata() {
		Assert.assertTrue(new SpewerStub().outputMetadata());
	}

	@Test
	public void testDefaultIsToOutputISODates() {
		Assert.assertTrue(new SpewerStub().isoDates());
	}

	@Test
	public void testWritesISO8601Dates() throws IOException {
		final SpewerStub spewer = new SpewerStub();
		final Document document = factory.create("test.txt");
		final Metadata metadata = document.getMetadata();
		final FieldNames fields = spewer.getFields();

		final String[] dates = {"2011-12-03+01:00", "2015-06-03"};
		final String[] isoDates = {"2011-12-03T12:00:00Z", "2015-06-03T12:00:00Z"};
		int i = 0;

		for (String date: dates) {
			metadata.set(Office.CREATION_DATE, date);
			spewer.writeMetadata(document);

			Assert.assertEquals(date, spewer.metadata.get(fields.forMetadata(Office.CREATION_DATE.getName())));
			Assert.assertEquals(isoDates[i++],
					spewer.metadata.get(fields.forMetadataISODate(Office.CREATION_DATE.getName())));

			// Reset the store of written metadata on each iteration.
			spewer.close();
		}
	}
}
