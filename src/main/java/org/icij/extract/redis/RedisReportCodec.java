package org.icij.extract.redis;

import org.icij.extract.core.Report;
import org.icij.extract.core.ReportResult;

import java.io.IOException;
import java.nio.file.Paths;

import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.client.codec.StringCodec;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Codec for a map of string keys to integer values.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisReportCodec extends StringCodec {

	private final Decoder<Object> resultDecoder = new Decoder<Object>() {

		@Override
		public Object decode(final ByteBuf buffer, final State state) {
			return ReportResult.get(Integer.valueOf(buffer.toString(CharsetUtil.UTF_8)));
		}
	};

	private final Decoder<Object> pathDecoder = new Decoder<Object>() {

		@Override
		public Object decode(final ByteBuf buffer, final State state) {
			return Paths.get(buffer.toString(CharsetUtil.UTF_8));
		}
	};

	private final Encoder resultEncoder = new Encoder() {

		@Override
		public byte[] encode(final Object in) throws IOException {
			return Integer.toString(((ReportResult) in).getValue()).getBytes(CharsetUtil.UTF_8);
		}
	};

	@Override
	public Decoder<Object> getMapKeyDecoder() {

		// Redisson calls getMapKeyDecoder for values and vice versa.
		// https://github.com/mrniko/redisson/issues/258
		return resultDecoder;
	}

    @Override
    public Decoder<Object> getMapValueDecoder() {
		return pathDecoder;
    }

    @Override
    public Encoder getMapValueEncoder() {
		return resultEncoder;
    }
}
