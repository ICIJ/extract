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

import org.apache.tika.exception.TikaException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.client.solrj.SolrServerException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

public class SolrSpewerTest extends SolrJettyTestBase {

	@Before
	public void setUp() throws Exception {
		getSolrClient().deleteByQuery("*:*");
		getSolrClient().commit(true, true);
	}

	@Test
	public void testWrite() throws IOException, TikaException, NoSuchAlgorithmException, SolrServerException {
		final Logger logger = Logger.getLogger("extract-test");

		final SolrSpewer spewer = new SolrSpewer(logger, getSolrClient());

		final Charset charset = StandardCharsets.UTF_8;
		final String buffer = "test";
		final Path path = FileSystems.getDefault().getPath("test-file.txt");
		final MessageDigest idDigest = MessageDigest.getInstance("SHA-256");
		final String pathHash = DatatypeConverter.printHexBinary(idDigest.digest(path.toString().getBytes(charset)));
		final ParsingReader reader = new ParsingReader(new ByteArrayInputStream(buffer.getBytes(charset)));

		spewer.setIdField("id", "SHA-256");
		spewer.setPathField("path");
		spewer.write(path, reader, charset);

		SolrDocument response = getSolrClient().getById("0");
		Assert.assertNull(response);

		response = getSolrClient().getById(pathHash);
		Assert.assertEquals(path.toString(), response.get("path"));
		Assert.assertEquals(buffer + "\n", response.get("content"));
	}
}
