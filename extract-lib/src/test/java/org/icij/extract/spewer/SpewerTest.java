package org.icij.extract.spewer;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.MetadataTransformer;
import org.icij.spewer.Spewer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class SpewerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	private class SpewerStub extends Spewer {

		private static final long serialVersionUID = 6023532612678893344L;
		final Map<String, String> metadata = new HashMap<>();

		SpewerStub() {
			super(new FieldNames());
		}

		@Override
		protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) {
		}

		void writeMetadata(final TikaDocument tikaDocument) throws IOException {
			final Metadata metadata = tikaDocument.getMetadata();

			new MetadataTransformer(metadata, fields).transform(this.metadata::put,
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
	public void testWritesISO8601Dates() throws IOException {
		final SpewerStub spewer = new SpewerStub();
		final TikaDocument tikaDocument = factory.create("test.txt");
		final Metadata metadata = tikaDocument.getMetadata();
		final FieldNames fields = spewer.getFields();

		// TODO: this should go in a separate test for the MetadataTransformer.
		final String[] dates = {"2011-12-03+01:00", "2015-06-03", "Tue Jan 27 17:03:21 2004", "19106-06-07T08:00:00Z"};
		final String[] isoDates = {"2011-12-03T12:00:00Z", "2015-06-03T12:00:00Z", "2004-01-27T17:03:21Z",
				"+19106-06-07T08:00:00Z"};
		int i = 0;

		for (String date: dates) {
			metadata.set(Office.CREATION_DATE, date);
			spewer.writeMetadata(tikaDocument);

			Assert.assertEquals(date, spewer.metadata.get(fields.forMetadata(Office.CREATION_DATE.getName())));
			Assert.assertEquals(isoDates[i++],
					spewer.metadata.get(fields.forMetadataISODate(Office.CREATION_DATE.getName())));

			// Reset the store of written metadata on each iteration.
			spewer.close();
		}
	}
}
