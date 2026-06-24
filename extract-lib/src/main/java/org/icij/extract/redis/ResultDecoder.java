package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.Report;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Decoder for converting the wire format {@code <statusCode>[|<serialized-exception>]} written by
 * {@link ResultEncoder} back into a {@link Report}.
 * <p>
 * The status code is read as every byte up to the {@code |} separator (or the end of the buffer),
 * so multi-digit codes such as {@code FAILURE_FATAL(10)} and {@code FAILURE_TIMEOUT(11)} decode
 * correctly. Reading a single byte (the previous behaviour) silently truncated those to the wrong
 * status. The buffer's readable region is delimited by reader/writer indices rather than capacity,
 * which a pooled allocator pads beyond the actual data length.
 */
public class ResultDecoder implements Decoder<Object> {
	Logger logger = LoggerFactory.getLogger(getClass());
	@Override
	public Object decode(final ByteBuf buffer, final State state) {
		final int start = buffer.readerIndex();
		final int end = buffer.writerIndex();
		final int separator = buffer.indexOf(start, end, (byte) '|');
		final int statusEnd = (separator == -1) ? end : separator;

		final String code = buffer.toString(start, statusEnd - start, StandardCharsets.US_ASCII);
		final ExtractionStatus extractionStatus = ExtractionStatus.parse(code);

		if (separator == -1) {
			return new Report(extractionStatus);
		}

		final byte[] exceptionPayload = new byte[end - (separator + 1)];
		buffer.getBytes(separator + 1, exceptionPayload);
		try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(exceptionPayload))) {
			Exception ex = (Exception) objectInputStream.readObject();
			return new Report(extractionStatus, ex);
		} catch (ClassNotFoundException|IOException e) {
			logger.warn("cannot read object : ", e);
			return new Report(extractionStatus, e);
		}
	}
}
