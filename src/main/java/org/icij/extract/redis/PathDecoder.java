package org.icij.extract.redis;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Decoder for converting a string to {@link java.nio.file.Path}.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class PathDecoder implements Decoder<Object> {

	@Override
	public Object decode(final ByteBuf buffer, final State state) {
		final Path path = Paths.get(buffer.toString(CharsetUtil.UTF_8));
		buffer.readerIndex(buffer.readableBytes());
		return path;
	}
}
