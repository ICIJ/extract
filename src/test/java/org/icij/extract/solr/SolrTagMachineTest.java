package org.icij.extract.solr;

import org.icij.extract.test.*;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.After;

public class SolrTagMachineTest extends SolrJettyTestBase {

	public static HttpSolrClient other;
	public static HttpSolrClient destination;

	@BeforeClass
	public static void setUpBeforeSolrTagMachineTest() throws Exception {
		other = createNewSolrClient("collection2");
		destination = createNewSolrClient("collection3");
	}

	@After
	public void tearDown() throws Exception {
		client.deleteByQuery("*:*");
		client.commit();
		other.deleteByQuery("*:*");
		other.commit();
		destination.deleteByQuery("*:*");
		destination.commit();
	}

	@Test
	public void testIntersect() throws IOException, SolrServerException, InterruptedException {
		final Map<String, String> literals = new HashMap<>();

		literals.put("metadata_in_a", "yes");

		final SolrMachineConsumer consumer = new SolrIntersectionConsumer(other, destination, literals);
		final SolrMachineProducer producer = new SolrMachineProducer(client, null);
		final SolrMachine machine =
			new SolrMachine(consumer, producer);

		// Commit four documents to core A.
		for (int i = 0; i < 4; i++) {
			SolrInputDocument input = new SolrInputDocument();

			input.setField("id", i);
			client.add(input);
		}

		client.commit();

		// Commit ten documents to core B.
		for (int i = 0; i < 10; i++) {
			SolrInputDocument input = new SolrInputDocument();

			input.setField("id", i);
			other.add(input);
		}

		other.commit();

		Assert.assertEquals(4, (Object) machine.call());
		machine.terminate();

		destination.commit();

		final SolrDocumentList results = destination.query(new SolrQuery("*:*"))
			.getResults();

		Assert.assertEquals(4, results.size());

		for (SolrDocument document : results) {
			Assert.assertEquals("yes", document.getFieldValue("metadata_in_a"));
		}

		// Test that each document was added.
		for (int i = 0; i < 4; i++) {
			String id = Integer.toString(i);
			boolean found = false;

			for (SolrDocument document : results) {
				if (id.equals(document.getFieldValue("id"))) {
					found = true;
					break;
				}
			}

			Assert.assertTrue(found);
		}
	}

	@Test
	public void testComplement() throws IOException, SolrServerException, InterruptedException {
		final Map<String, String> literals = new HashMap<>();

		literals.put("metadata_in_a", "no");

		final SolrClient other = createNewSolrClient("collection2");
		final SolrClient destination = createNewSolrClient("collection3");

		final SolrMachineConsumer consumer = new SolrComplementConsumer(other, destination, literals);
		final SolrMachineProducer producer = new SolrMachineProducer(client, null);
		final SolrMachine machine =
			new SolrMachine(consumer, producer);

		// Commit ten documents to core A.
		for (int i = 0; i < 10; i++) {
			SolrInputDocument input = new SolrInputDocument();

			input.setField("id", i);
			client.add(input);
		}

		client.commit();

		// Commit 4 documents to core B.
		for (int i = 0; i < 4; i++) {
			SolrInputDocument input = new SolrInputDocument();

			input.setField("id", i);
			other.add(input);
		}

		other.commit();

		Assert.assertEquals(10, (Object) machine.call());
		machine.terminate();

		destination.commit();

		final SolrDocumentList results = destination.query(new SolrQuery("*:*"))
			.getResults();

		Assert.assertEquals(6, results.size());

		for (SolrDocument document : results) {
			Assert.assertEquals("no", document.getFieldValue("metadata_in_a"));
		}

		// Test that each document was added.
		for (int i = 5; i < 10; i++) {
			String id = Integer.toString(i);
			boolean found = false;

			for (SolrDocument document : results) {
				if (id.equals(document.getFieldValue("id"))) {
					found = true;
					break;
				}
			}

			Assert.assertTrue(found);
		}
	}
}
