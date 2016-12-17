package org.icij.extract.redis;

import java.io.IOException;

import org.icij.extract.extractor.ExtractionStatus;

import org.redisson.client.protocol.Encoder;
import io.netty.util.CharsetUtil;

/**
 * Decoder for converting a string to a {@link ExtractionStatus}.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ResultEncoder implements Encoder {

	@Override
	public byte[] encode(final Object in) throws IOException {
		return Integer.toString(((ExtractionStatus) in).getCode()).getBytes(CharsetUtil.UTF_8);
	}
}
