package org.icij.extract.redis;

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
public class StringIntegerMapCodec extends StringCodec {

	public final Decoder<Object> integerDecoder = new Decoder<Object>() {

		@Override
		public Object decode(ByteBuf buf, State state) {
			return Integer.valueOf(buf.toString(CharsetUtil.UTF_8));
		}
	};

    @Override
    public Decoder<Object> getMapValueDecoder() {
		return integerDecoder;
    }
}
