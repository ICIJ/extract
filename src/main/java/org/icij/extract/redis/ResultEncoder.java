package org.icij.extract.redis;

import java.io.IOException;

import org.icij.extract.extractor.ExtractionResult;

import org.redisson.client.protocol.Encoder;
import io.netty.util.CharsetUtil;

/**
 * Decoder for converting a string to a {@link ExtractionResult}.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ResultEncoder implements Encoder {

	@Override
	public byte[] encode(final Object in) throws IOException {
		return Integer.toString(((ExtractionResult) in).getValue()).getBytes(CharsetUtil.UTF_8);
	}
}
