package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PathDecoder implements Decoder<Object> {
	private final Charset charset;

	public PathDecoder(final Charset charset) {
		this.charset = charset;
	}

	@Override
	public Path decode(final ByteBuf buffer, final State state) {
		final Path path = Paths.get(buffer.toString(charset));
		buffer.readerIndex(buffer.readableBytes());
		return path;
	}
}
