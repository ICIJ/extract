package org.icij.extract.spewer;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.icij.extract.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ElasticsearchSpewer extends Spewer implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSpewer.class);
    private final Client client;
    private static final String ES_INDEX_NAME = "datashare";
    private static final String ES_INDEX_TYPE = "doc";
    private static final String ES_DOC_TYPE_FIELD = "type";
    private static final String ES_JOIN_FIELD = "join";
    private static final String ES_CONTENT_FIELD = "content";

    public ElasticsearchSpewer(final Client client, final FieldNames fields) {
       		super(fields);
       		this.client = client;
       	}

    @Override
    public void write(final Document document, final Reader reader) throws IOException {
        IndexRequest req = new IndexRequest(ES_INDEX_NAME, ES_INDEX_TYPE, document.getId());
        req = req.source(generateJsonFrom(document, reader));
        try {
            client.index(req).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("interrupted execution of request", e);
        }
    }

    private Map<String, Object> generateJsonFrom(final Document document, final Reader reader) throws IOException {
        Map<String, Object> jsonDocument = new HashMap<>();
        new MetadataTransformer(document.getMetadata(), fields).
                transform(new MapValueConsumer(jsonDocument), new MapValuesConsumer(jsonDocument));
        jsonDocument.put(ES_DOC_TYPE_FIELD, "document");
        jsonDocument.put(ES_CONTENT_FIELD, toString(reader));
        return jsonDocument;
    }

    @Override
    public void writeMetadata(Document document) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    static class MapValueConsumer implements MetadataTransformer.ValueConsumer {
        private final Map<String, Object> map;
        MapValueConsumer(final Map<String, Object> map) { this.map = map;}
        @Override
        public void accept(String name, String value) throws IOException {
            map.put(name, value);
        }
    }

    static class MapValuesConsumer implements MetadataTransformer.ValueArrayConsumer {
        private final Map<String, Object> map;
        MapValuesConsumer(Map<String, Object> jsonDocument) { map = jsonDocument;}
        @Override
        public void accept(String name, String[] values) throws IOException {
            map.put(name, String.join(",", values));
        }
    }
}
