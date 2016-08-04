package org.icij.extract.solr;

import org.icij.extract.core.ParsingReader;
import org.icij.extract.core.TextParsingReader;
import org.icij.extract.test.*;
import org.icij.extract.solr.SolrSpewer;

import java.util.Map;
import java.util.HashMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.security.NoSuchAlgorithmException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.TikaException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrServerException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.After;

public class SolrSpewerTest extends SolrJettyTestBase {

	@After
	public void tearDown() throws Exception {
		client.deleteByQuery("*:*");
		client.commit(true, true);
		client.optimize(true, true);
	}

	@Test
	public void testWrite() throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException {
		final SolrSpewer spewer = new SolrSpewer(logger, client);

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Path path = Paths.get("test-file.txt");
		final ParsingReader reader = new TextParsingReader(logger, new ByteArrayInputStream(buffer.getBytes(charset)));

		spewer.setIdAlgorithm("SHA-256");
		spewer.write(path, new Metadata(), reader);
		client.commit(true, true);

		SolrDocument response = client.getById("0");
		Assert.assertNull(response);

		final String pathHash = spewer.generateId(path);
		response = client.getById(pathHash);
		Assert.assertEquals(path.toString(), response.get("path"));
		Assert.assertEquals(buffer + "\n", response.get("content"));
	}

	@Test
	public void testWriteMetadata()
		throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException, InterruptedException {
		final SolrSpewer spewer = new SolrSpewer(logger, client);

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Path path = Paths.get("test/file.txt");
		final ParsingReader reader = new TextParsingReader(logger, new ByteArrayInputStream(buffer.getBytes(charset)));
		final Metadata metadata = new Metadata();

		spewer.setIdAlgorithm("SHA-256");
		spewer.outputMetadata(true);

		final String length = Integer.toString(buffer.getBytes(charset).length);
		metadata.set("Content-Length", length);
		metadata.set("Content-Type", "text/plain; charset=UTF-8");

		spewer.write(path, metadata, reader);
		client.commit(true, true);
		client.optimize(true, true);

		final String pathHash = spewer.generateId(path);
		final SolrDocument response = client.getById(pathHash);
		Assert.assertEquals(path.toString(), response.getFieldValue("path"));
		Assert.assertEquals(length, response.getFieldValue("metadata_content_length"));
		Assert.assertEquals("text/plain", response.getFieldValue("base_type"));
		Assert.assertEquals("text/plain; charset=UTF-8", response.getFieldValue("metadata_content_type"));
		Assert.assertEquals("test", response.getFieldValue("parent_path"));
	}

	@Test
	public void testWriteTags()
		throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException, InterruptedException {
		final SolrSpewer spewer = new SolrSpewer(logger, client);

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Path path = Paths.get("test/file.txt");
		final ParsingReader reader = new TextParsingReader(logger, new ByteArrayInputStream(buffer.getBytes(charset)));
		final Metadata metadata = new Metadata();
		final Map<String, String> tags = new HashMap<>();

		tags.put("batch", "1");

		spewer.setIdAlgorithm("SHA-256");
		spewer.outputMetadata(true);
		spewer.setTags(tags);

		spewer.write(path, metadata, reader);
		client.commit(true, true);
		client.optimize(true, true);

		final String pathHash = spewer.generateId(path);
		final SolrDocument response = client.getById(pathHash);
		Assert.assertEquals(path.toString(), response.getFieldValue("path"));
		Assert.assertEquals("1", response.getFieldValue("batch"));
	}
}
