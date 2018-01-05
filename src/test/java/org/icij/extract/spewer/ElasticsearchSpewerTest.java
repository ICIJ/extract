package org.icij.extract.spewer;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.parser.ParsingReader;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class ElasticsearchSpewerTest {
	private static Client client;
	private ElasticsearchSpewer spewer = new ElasticsearchSpewer(client, new FieldNames());
	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	@BeforeClass
	public static void setUpClass() throws Exception {
		System.setProperty("es.set.netty.runtime.available.processors", "false");
		Settings settings = Settings.builder().put("cluster.name", "docker-cluster").build();
		client = new PreBuiltTransportClient(settings).addTransportAddress(
				new TransportAddress(InetAddress.getByName("elasticsearch"), 9300));
		client.admin().indices().create(new CreateIndexRequest("datashare"));
	}

	@Test
	public void testSimpleWrite() throws Exception {
		final Document document = factory.create(Paths.get("test-file.txt"));
		final ParsingReader reader = new ParsingReader(new ByteArrayInputStream("test".getBytes()));

		spewer.write(document, reader);

    	GetResponse documentFields = client.get(new GetRequest("datashare", "doc", document.getId())).get();
		assertEquals(document.getId(), documentFields.getId());
	}
}
