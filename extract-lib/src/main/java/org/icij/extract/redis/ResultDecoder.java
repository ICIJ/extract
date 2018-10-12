package org.icij.extract.redis;

import org.icij.extract.extractor.ExtractionStatus;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * Decoder for converting a string to a {@link ExtractionStatus}.
 *
 *
 */
public class ResultDecoder implements Decoder<Object> {

	@Override
	public Object decode(final ByteBuf buffer, final State state) {
		return ExtractionStatus.parse(Integer.valueOf(buffer.toString(CharsetUtil.UTF_8)));
	}
}
