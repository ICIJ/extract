package org.icij.extract.redis;

import org.icij.extract.document.Document;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;
import java.nio.charset.Charset;

public class DocumentEncoder implements Encoder {

	private final Charset charset;

	public DocumentEncoder(final Charset charset) {
		this.charset = charset;
	}

	@Override
	public byte[] encode(final Object in) throws IOException {
		return ((Document) in).getPath().toString().getBytes(charset);
	}
}
