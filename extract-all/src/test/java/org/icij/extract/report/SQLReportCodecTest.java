package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.kaxxa.sql.SQLMapCodec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class SQLReportCodecTest {

	private final DocumentFactory documentFactory = new DocumentFactory().withIdentifier(new PathIdentifier());

	@Test
	public void testEncodeKey() throws Throwable {
		final SQLMapCodec<Document, Report> codec = new SQLReportCodec(documentFactory);
		final Document document = documentFactory.create("a", "path/to/file");

		final Map<String, Object> map = codec.encodeKey(document);

		Assert.assertEquals("path/to/file", map.get("path"));
	}

	@Test
	public void testEncodeValue() throws Throwable {
		final Report report = new Report(ExtractionStatus.FAILURE_NOT_PARSED, new IOException("Failed to parse."));
		final SQLMapCodec<Document, Report> codec = new SQLReportCodec(documentFactory);

		final Map<String, Object> map = codec.encodeValue(report);

		Assert.assertEquals("FAILURE_NOT_PARSED", map.get("extraction_status"));

		final Object exception = map.get("exception");

		Assert.assertNotNull(exception);
		Assert.assertTrue(exception.toString().contains("Failed to parse."));
	}
}
