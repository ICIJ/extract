package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;
import org.icij.extract.extractor.ExtractionStatus;
import org.redisson.client.protocol.Encoder;

public class ResultEncoder implements Encoder {
	@Override
	public ByteBuf encode(final Object in) {
		byte[] payload = Integer.toString(((ExtractionStatus) in).getCode()).getBytes(CharsetUtil.UTF_8);
		ByteBuf out = ByteBufAllocator.DEFAULT.buffer(payload.length);
		out.writeBytes(payload);
		return out;
	}
}
