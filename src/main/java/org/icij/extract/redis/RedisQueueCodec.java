package org.icij.extract.redis;

import org.icij.extract.core.Queue;

import java.nio.file.Paths;

import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.codec.StringCodec;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Codec for a map of string keys to integer values.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisQueueCodec extends StringCodec {

	private final Decoder<Object> pathDecoder = new Decoder<Object>() {

		@Override
		public Object decode(final ByteBuf buffer, final State state) {
			return Paths.get(buffer.toString(CharsetUtil.UTF_8));
		}
	};

	@Override
	public Decoder<Object> getValueDecoder() {
		return pathDecoder;
	}
}
