package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.redisson.client.protocol.Encoder;

import java.nio.charset.Charset;

public class PathEncoder implements Encoder {

	private final Charset charset;

	public PathEncoder(final Charset charset) {
		this.charset = charset;
	}

	@Override
	public ByteBuf encode(final Object in) {
		byte[] payload = in.toString().getBytes(charset);
		ByteBuf out = ByteBufAllocator.DEFAULT.buffer(payload.length);
		out.writeBytes(payload);
		return out;
	}
}
