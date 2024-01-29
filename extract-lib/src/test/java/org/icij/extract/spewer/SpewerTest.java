package org.icij.extract.spewer;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.MetadataTransformer;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class SpewerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	private static class SpewerStub extends Spewer {

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
		assertEquals(StandardCharsets.UTF_8, new SpewerStub().getOutputEncoding());
	}

	@Test
	public void testConfigureWithDefaultValues() {
		Spewer configured = new SpewerStub().configure(Options.from(new HashMap<>()));
		assertEquals(configured.getOutputEncoding(), StandardCharsets.UTF_8);
		assertEquals(configured.outputMetadata(), false);
		assertEquals(configured.getTags(), new HashMap<>());
	}

	@Test
	public void testSetOutputEncoding() {
		final Spewer spewer = new SpewerStub();

		spewer.setOutputEncoding(StandardCharsets.US_ASCII);
		assertEquals(StandardCharsets.US_ASCII, spewer.getOutputEncoding());
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

			assertEquals(date, spewer.metadata.get(fields.forMetadata(Office.CREATION_DATE.getName())));
			assertEquals(isoDates[i++],
					spewer.metadata.get(fields.forMetadataISODate(Office.CREATION_DATE.getName())));

			// Reset the store of written metadata on each iteration.
			spewer.close();
		}
	}

    @Test
    public void testSpewDocumentWithoutBlockedMetadata() throws IOException {
		final SpewerStub spewer = new SpewerStub();
		final TikaDocument tikaDocument = factory.create("test.txt");
		final Metadata metadata = tikaDocument.getMetadata();
		metadata.set("bar", "bar");
		metadata.set("unknown_tag_0x", "foo");
		spewer.writeMetadata(tikaDocument);
		// Those value should not be blocked
		assertEquals(spewer.metadata.get("tika_metadata_resourcename"), "test.txt");
		assertEquals(spewer.metadata.get("tika_metadata_bar"), "bar");
		// But this one should
		Assert.assertNull(spewer.metadata.getOrDefault("tika_metadata_unknown_tag_0x", null), null);
	}
}
