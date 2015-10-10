package org.icij.extract.redis;

import org.redisson.client.codec.StringCodec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.Decoder;

/**
 * Codec for a map of string keys to integer or long values.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class StringLongMapCodec extends StringCodec {

    @Override
    public Decoder<Object> getMapValueDecoder() {
        return LongCodec.INSTANCE.getValueDecoder();
    }
}
