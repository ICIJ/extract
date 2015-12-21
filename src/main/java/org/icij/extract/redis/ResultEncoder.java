package org.icij.extract.redis;

import java.io.IOException;

import org.icij.extract.core.ReportResult;

import org.redisson.client.protocol.Encoder;
import io.netty.util.CharsetUtil;

/**
 * Decoder for converting a string to a {@link ReportResult}.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ResultEncoder implements Encoder {

	@Override
	public byte[] encode(final Object in) throws IOException {
		return Integer.toString(((ReportResult) in).getValue()).getBytes(CharsetUtil.UTF_8);
	}
}
