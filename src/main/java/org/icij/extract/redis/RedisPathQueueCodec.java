package org.icij.extract.redis;

import org.redisson.client.protocol.Decoder;
import org.redisson.client.codec.StringCodec;

/**
 * Codec for a map of string keys to integer values.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
class RedisPathQueueCodec extends StringCodec {

	private final Decoder<Object> pathDecoder = new PathDecoder();

	@Override
	public Decoder<Object> getValueDecoder() {
		return pathDecoder;
	}
}
