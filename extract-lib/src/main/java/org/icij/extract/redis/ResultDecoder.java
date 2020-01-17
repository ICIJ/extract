package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.Report;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Decoder for converting a string to a {@link ExtractionStatus}.
 *
 *
 */
public class ResultDecoder implements Decoder<Object> {
	Logger logger = LoggerFactory.getLogger(getClass());
	@Override
	public Object decode(final ByteBuf buffer, final State state) throws IOException {
		String result = new String (new char[] {(char) buffer.readByte()});
		ExtractionStatus extractionStatus = ExtractionStatus.parse(result);
		if (buffer.capacity() == 1) {
			return new Report(extractionStatus);
		} else {
			buffer.readByte(); // pipe char |
			ByteBuf exceptionPayload = buffer.getBytes(0, new byte[buffer.capacity() - 2]);
			try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteBufInputStream(exceptionPayload))) {
				Exception ex = (Exception) objectInputStream.readObject();
				return new Report(extractionStatus, ex);
			} catch (ClassNotFoundException e) {
				logger.warn("cannot read object : ", e);
				return new Report(extractionStatus);
			}
		}
	}
}
