package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.icij.extract.document.TikaDocument;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;
import java.nio.charset.Charset;

public class DocumentEncoder implements Encoder {

	private final Charset charset;

	public DocumentEncoder(final Charset charset) {
		this.charset = charset;
	}

	@Override
	public ByteBuf encode(final Object in) throws IOException {
		byte[] payload = ((TikaDocument) in).getPath().toString().getBytes(charset);
		ByteBuf out = ByteBufAllocator.DEFAULT.buffer(payload.length);
		out.writeBytes(payload);
		return out;
	}
}
