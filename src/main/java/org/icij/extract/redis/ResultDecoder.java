package org.icij.extract.redis;

import org.icij.extract.core.ExtractionResult;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Decoder for converting a string to a {@link ExtractionResult}.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ResultDecoder implements Decoder<Object> {

	@Override
	public Object decode(final ByteBuf buffer, final State state) {
		return ExtractionResult.get(Integer.valueOf(buffer.toString(CharsetUtil.UTF_8)));
	}
}
