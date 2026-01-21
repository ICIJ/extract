package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import org.icij.extract.report.Report;
import org.redisson.client.protocol.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class ResultEncoder implements Encoder {
	Logger logger = LoggerFactory.getLogger(getClass());
	@Override
	public ByteBuf encode(final Object in) throws IOException {
		Report report = (Report) in;
		ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
		out.writeBytes(String.valueOf(report.getStatus().getCode()).getBytes());

		ByteBuf exceptionPayload = ByteBufAllocator.DEFAULT.buffer();
		try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new ByteBufOutputStream(exceptionPayload))) {
			report.getException().ifPresent(e -> {
				out.writeBytes("|".getBytes());
				try {
					objectOutputStream.writeObject(e);
					out.writeBytes(exceptionPayload);
				} catch (IOException ex) {
					logger.error("exception when serializing exception :", e);
				}
			});
		} finally {
			exceptionPayload.release();
		}
		return out;
	}
}
