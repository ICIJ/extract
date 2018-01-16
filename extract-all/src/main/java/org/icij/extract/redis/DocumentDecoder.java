package org.icij.extract.redis;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.icij.extract.document.DocumentFactory;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;

import io.netty.buffer.ByteBuf;

/**
 * Decoder for converting a string to {@link Path}.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class DocumentDecoder implements Decoder<Object> {

	private final DocumentFactory factory;
	private final Charset charset;

	public DocumentDecoder(final DocumentFactory factory, final Charset charset) {
		this.factory = factory;
		this.charset = charset;
	}

	@Override
	public Object decode(final ByteBuf buffer, final State state) {
		final Path path = Paths.get(buffer.toString(charset));

		buffer.readerIndex(buffer.readableBytes());
		return factory.create(path);
	}
}
