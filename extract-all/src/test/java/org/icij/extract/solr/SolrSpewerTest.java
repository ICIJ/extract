package org.icij.extract.solr;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.parser.ParsingReader;
import org.icij.extract.spewer.FieldNames;
import org.icij.extract.spewer.SolrSpewer;
import org.icij.extract.test.*;

import java.util.Map;
import java.util.HashMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.security.NoSuchAlgorithmException;

import org.apache.tika.exception.TikaException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrServerException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.After;

public class SolrSpewerTest extends SolrJettyTestBase {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	@After
	public void tearDown() throws Exception {
		client.deleteByQuery("*:*");
		client.commit(true, true);
		client.optimize(true, true);
	}

	@Test
	public void testWrite() throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException {
		final SolrSpewer spewer = new SolrSpewer(client, new FieldNames());

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Document document = factory.create(Paths.get("test-file.txt"));
		final ParsingReader reader = new ParsingReader(new ByteArrayInputStream(buffer.getBytes(charset)));

		spewer.write(document, reader);
		client.commit(true, true);

		SolrDocument response = client.getById("0");
		Assert.assertNull(response);

		response = client.getById(document.getId());
		Assert.assertEquals(document.getPath().toString(), response.get("path"));
		Assert.assertEquals(buffer + "\n", response.get("content"));
	}

	@Test
	public void testWriteMetadata()
		throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException, InterruptedException {
		final SolrSpewer spewer = new SolrSpewer(client, new FieldNames());

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Document document = factory.create(Paths.get("test/file.txt"));
		final ParsingReader reader = new ParsingReader(new ByteArrayInputStream(buffer.getBytes(charset)));

		spewer.outputMetadata(true);

		final String length = Integer.toString(buffer.getBytes(charset).length);
		document.getMetadata().set("Content-Length", length);
		document.getMetadata().set("Content-Type", "text/plain; charset=UTF-8");

		spewer.write(document, reader);
		client.commit(true, true);
		client.optimize(true, true);

		final SolrDocument response = client.getById(document.getId());

		Assert.assertEquals(document.getPath().toString(), response.getFieldValue("path"));
		Assert.assertEquals(length, response.getFieldValue("metadata_content_length"));
		Assert.assertEquals("text/plain", response.getFieldValue("metadata_base_type"));
		Assert.assertEquals("text/plain; charset=UTF-8", response.getFieldValue("metadata_content_type"));
		Assert.assertEquals("test", response.getFieldValue("metadata_parent_path"));
	}

	@Test
	public void testWriteTags()
		throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException, InterruptedException {
		final SolrSpewer spewer = new SolrSpewer(client, new FieldNames());

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Document document = factory.create(Paths.get("test/file.txt"));
		final ParsingReader reader = new ParsingReader(new ByteArrayInputStream(buffer.getBytes(charset)));
		final Map<String, String> tags = new HashMap<>();

		tags.put("batch", "1");

		spewer.outputMetadata(true);
		spewer.setTags(tags);

		spewer.write(document, reader);
		client.commit(true, true);
		client.optimize(true, true);

		final SolrDocument response = client.getById(document.getId());
		Assert.assertEquals(document.getPath().toString(), response.getFieldValue("path"));
		Assert.assertEquals("1", response.getFieldValue("batch"));
	}
}
