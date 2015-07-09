package org.icij.extract.core;

import java.util.logging.Logger;

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
import org.apache.solr.client.solrj.SolrServerException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

public class SolrSpewerTest extends SolrJettyTestBase {

	private final Logger logger = Logger.getLogger("extract-test");

	@Before
	public void setUp() throws Exception {
		getSolrClient().deleteByQuery("*:*");
		getSolrClient().commit(true, true);
	}

	@Test
	public void testWrite() throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException {
		final SolrSpewer spewer = new SolrSpewer(logger, getSolrClient());

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Path path = FileSystems.getDefault().getPath("test-file.txt");
		final MessageDigest idDigest = MessageDigest.getInstance("SHA-256");
		final String pathHash = DatatypeConverter.printHexBinary(idDigest.digest(path.toString().getBytes(charset)));
		final ParsingReader reader = new TextParsingReader(logger, new ByteArrayInputStream(buffer.getBytes(charset)));

		spewer.setIdAlgorithm("SHA-256");
		spewer.write(path, new Metadata(), reader, charset);

		SolrDocument response = getSolrClient().getById("0");
		Assert.assertNull(response);

		response = getSolrClient().getById(pathHash);
		Assert.assertEquals(path.toString(), response.get("path"));
		Assert.assertEquals(buffer + "\n", response.get("content"));
	}

	@Test
	public void testWriteMetadata() throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException {
		final SolrSpewer spewer = new SolrSpewer(logger, getSolrClient());

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Path path = FileSystems.getDefault().getPath("test-file.txt");
		final MessageDigest idDigest = MessageDigest.getInstance("SHA-256");
		final String pathHash = DatatypeConverter.printHexBinary(idDigest.digest(path.toString().getBytes(charset)));
		final ParsingReader reader = new TextParsingReader(logger, new ByteArrayInputStream(buffer.getBytes(charset)));
		final Metadata metadata = new Metadata();

		spewer.setIdAlgorithm("SHA-256");
		spewer.outputMetadata(true);

		// Need to commit since the file from the previous test is being updated.
		spewer.setCommitInterval(1);

		final String length = Integer.toString(buffer.getBytes(charset).length);
		metadata.set("Content-Length", length);

		spewer.write(path, metadata, reader, charset);

		final SolrDocument response = getSolrClient().getById(pathHash);
		Assert.assertEquals(length, response.get("metadata_content_length"));
	}
}
