package org.icij.extract.redis;

import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.client.codec.StringCodec;

/**
 * Codec for a map of string keys to integer values.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisReportCodec extends StringCodec {

	private final Decoder<Object> resultDecoder = new ResultDecoder();
	private final Decoder<Object> pathDecoder = new PathDecoder();
	private final Encoder resultEncoder = new ResultEncoder();

	@Override
	public Decoder<Object> getMapKeyDecoder() {
		return pathDecoder;
	}

    @Override
    public Decoder<Object> getMapValueDecoder() {
		return resultDecoder;
    }

    @Override
    public Encoder getMapValueEncoder() {
		return resultEncoder;
    }
}
