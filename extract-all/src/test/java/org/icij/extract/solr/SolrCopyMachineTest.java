package org.icij.extract.solr;

import org.icij.extract.test.*;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.After;

public class SolrCopyMachineTest extends SolrJettyTestBase {

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
			input.setField("metadata_bad_trie_test", "ERROR:SCHEMA-INDEX-MISMATCH,stringValue=123");
			input.setField("metadata_setA_" + i, "value " + i);
		}

		return input;
	}

	@Test
	public void testCopy() throws IOException, SolrServerException, InterruptedException {
		final Map<String, String> map = new HashMap<>();

		final SolrMachineConsumer consumer = new SolrCopyConsumer(client, map);
		final SolrMachineProducer producer = new SolrMachineProducer(client, map.keySet());
		final SolrMachine machine =
			new SolrMachine(consumer, producer);

		final int documents = 10;
		final int fields = 10;

		for (int i = 0; i < documents; i++) {
			SolrInputDocument input = createDocument(i, fields);
			client.add(input);
		}

		client.commit();

		map.put("metadata_bad_trie_test", "metadata_bad_trie_test");
		for (int i = 0; i < fields; i++) {
			map.put("metadata_setA_" + i, "metadata_setB_" + i);
		}

		Assert.assertEquals(documents, (Object) machine.call());
		machine.terminate();
		client.commit();

		final SolrDocumentList results = client.query(new SolrQuery("*:*"))
			.getResults();

		Assert.assertEquals(documents, results.size());

		// Test that each document was added.
		for (int i = 0; i < documents; i++) {
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

		// Test that in each document added, all the fields were copied.
		for (SolrDocument document : results) {

			// Test that the bad field was fixed.
			Assert.assertEquals("123", document.getFieldValue("metadata_bad_trie_test"));

			for (int i = 0; i < fields; i++) {
				Assert.assertEquals("value " + i, document.getFieldValue("metadata_setA_" + i));
				Assert.assertEquals("value " + i, document.getFieldValue("metadata_setB_" + i));
			}
		}
	}
}
