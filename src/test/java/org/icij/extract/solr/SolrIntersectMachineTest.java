package org.icij.extract.solr;

import org.icij.extract.test.*;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.After;

public class SolrIntersectMachineTest extends SolrJettyTestBase {

	@After
	public void tearDown() throws Exception {
		client.deleteByQuery("*:*");
		client.commit(true, true);
		client.optimize(true, true);
	}

	private SolrInputDocument createDocument(final int id, final int fields) {
		final SolrInputDocument input = new SolrInputDocument();

		for (int i = 0; i < fields; i++) {
			input.setField("id", id);
			input.setField("metadata_setA_" + i, "value " + i);
		}

		return input;
	}

	@Test
	public void testTag() throws IOException, SolrServerException, InterruptedException {
		final Map<String, String> literals = new HashMap<String, String>();

		literals.put("metadata_in_a", "yes");

		final SolrClient a = client;
		final SolrClient b = createNewSolrClient("collection2");
		final SolrClient c = createNewSolrClient("collection3");

		final SolrMachineConsumer consumer = new SolrIntersectionConsumer(logger, a, b, c, literals);
		final SolrMachineProducer producer = new SolrMachineProducer(logger, client, null);
		final SolrMachine machine =
			new SolrMachine(logger, consumer, producer);

		final int fields = 10;

		// Commit five documents to core A.
		for (int i = 0; i < 5; i++) {
			SolrInputDocument input = new SolrInputDocument();

			input.setField("id", i);
			a.add(input);
		}

		a.commit();

		// Commit ten documents to core B.
		for (int i = 0; i < 10; i++) {
			SolrInputDocument input = new SolrInputDocument();

			input.setField("id", i);
			b.add(input);
		}

		Assert.assertEquals(5, (Object) machine.call());
		machine.terminate();

		c.commit();

		final SolrDocumentList results = c.query(new SolrQuery("*:*"))
			.getResults();

		Assert.assertEquals(5, results.size());

		for (SolrDocument document : results) {
			Assert.assertEquals("yes", document.getFieldValue("metadata_in_a"));
		}

		// Test that each document was added.
		for (int i = 0; i < 5; i++) {
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
