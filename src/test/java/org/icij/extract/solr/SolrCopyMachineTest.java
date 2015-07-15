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
			input.setField("metadata_setA_" + i, "value " + i);
		}

		return input;
	}

	@Test
	public void testCopy() throws IOException, SolrServerException {
		final Map<String, String> map = new HashMap<String, String>();
		final SolrCopyMachine machine = new SolrCopyMachine(logger, client, map);
		final int documents = 10;
		final int fields = 10;

		for (int i = 0; i < documents; i++) {
			SolrInputDocument input = createDocument(i, fields);
			client.add(input);
		}

		client.commit();

		for (int i = 0; i < fields; i++) {
			map.put("metadata_setA_" + i, "metadata_setB_" + i);
		}

		machine.setBatchSize(5);
		Assert.assertEquals(documents, machine.copy());
		machine.shutdown();
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
			for (int i = 0; i < fields; i++) {
				Assert.assertEquals("value " + i, document.getFieldValue("metadata_setA_" + i));
				Assert.assertEquals("value " + i, document.getFieldValue("metadata_setB_" + i));
			}
		}
	}
}
