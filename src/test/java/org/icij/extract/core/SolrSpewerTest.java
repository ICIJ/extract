package org.icij.extract.core;

import org.icij.extract.test.*;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.FileSystems;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.TikaException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrClient;
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
		final Path path = FileSystems.getDefault().getPath("test-file.txt");
		final MessageDigest idDigest = MessageDigest.getInstance("SHA-256");
		final String pathHash = DatatypeConverter.printHexBinary(idDigest.digest(path.toString().getBytes(charset)));
		final ParsingReader reader = new TextParsingReader(logger, new ByteArrayInputStream(buffer.getBytes(charset)));

		spewer.setIdAlgorithm("SHA-256");
		spewer.write(path, new Metadata(), reader, charset);
		client.commit(true, true);

		SolrDocument response = client.getById("0");
		Assert.assertNull(response);

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
		final Path path = FileSystems.getDefault().getPath("test-file.txt");
		final MessageDigest idDigest = MessageDigest.getInstance("SHA-256");
		final String pathHash = DatatypeConverter.printHexBinary(idDigest.digest(path.toString().getBytes(charset)));
		final ParsingReader reader = new TextParsingReader(logger, new ByteArrayInputStream(buffer.getBytes(charset)));
		final Metadata metadata = new Metadata();

		spewer.setIdAlgorithm("SHA-256");
		spewer.outputMetadata(true);

		final String length = Integer.toString(buffer.getBytes(charset).length);
		metadata.set("Content-Length", length);

		spewer.write(path, metadata, reader, charset);
		client.commit(true, true);
		client.optimize(true, true);

		final SolrDocument response = client.getById(pathHash);
		Assert.assertEquals(length, response.getFieldValue("metadata_content_length"));
	}
}
