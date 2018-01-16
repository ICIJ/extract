package org.icij.extract.spewer;

import org.apache.commons.io.TaggedIOException;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RESTSpewer extends Spewer implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(RESTSpewer.class);
	private static final long serialVersionUID = -1551290699012748766L;

	private final CloseableHttpClient client;
	private final URI uri;

	RESTSpewer(final FieldNames fields, final CloseableHttpClient client, final URI uri) {
		super(fields);
		this.client = client;
		this.uri = uri;
	}

	@Override
	public void write(final Document document, final Reader reader) throws IOException {
		final HttpPut put = new HttpPut(uri.resolve(document.getId()));
		final List<NameValuePair> params = new ArrayList<>();

		params.add(new BasicNameValuePair(fields.forId(), document.getId()));
		params.add(new BasicNameValuePair(fields.forPath(), document.getPath().toString()));
		params.add(new BasicNameValuePair(fields.forText(), toString(reader)));

		if (outputMetadata) {
			parametrizeMetadata(document.getMetadata(), params);
		}

		put.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
		put(put);
	}

	@Override
	public void writeMetadata(final Document document) throws IOException {
		final HttpPut put = new HttpPut(uri.resolve(document.getId()));
		final List<NameValuePair> params = new ArrayList<>();

		parametrizeMetadata(document.getMetadata(), params);
		put.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
		put(put);
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

	private void parametrizeMetadata(final Metadata metadata, final List<NameValuePair> params) throws IOException {
		new MetadataTransformer(metadata, fields).transform((name, value)-> params.add(new BasicNameValuePair(name, value)),
				(name, values)-> {
			for (String value: values) {
				params.add(new BasicNameValuePair(name, value));
			}
		});
	}

	private void put(final HttpPut put) throws IOException {
		logger.info(String.format("Writing to \"%s\".", put.getURI()));

		try (final CloseableHttpResponse response = client.execute(put)) {
			final int code = response.getStatusLine().getStatusCode();

			if (code < 200 || code >= 300) {
				throw new TaggedIOException(new IOException(String.format("Unexpected response code: %d", code)), this);
			}
		}
	}
}
