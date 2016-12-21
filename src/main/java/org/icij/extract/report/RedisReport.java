package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.redis.*;
import org.icij.extract.extractor.ExtractionStatus;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.icij.task.Options;

import org.redisson.RedissonMap;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.client.RedisOutOfMemoryException;
import org.redisson.command.CommandSyncService;
import org.redisson.connection.ConnectionManager;

/**
 * A {@link Report} using Redis as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisReport extends RedissonMap<Document, ExtractionStatus> implements Report {

	/**
	 * The default name for a report in Redis.
	 */
	private static final String DEFAULT_NAME = "extract:report";

	private final ConnectionManager connectionManager;

	/**
	 * Create a Redis-backed report using the provided configuration.
	 *
	 * @param options options for connecting to Redis
	 */
	public RedisReport(final DocumentFactory factory, final Options<String> options) {
		this(factory, new ConnectionManagerFactory().withOptions(options).create(),
				options.get("report-name").value().orElse(DEFAULT_NAME),
				options.get("charset").parse().asCharset().orElse(StandardCharsets.UTF_8));
	}

	/**
	 * Instantiate a new Redis-backed report using the provided connection manager and name.
	 *
	 * @param connectionManager instantiated using {@link ConnectionManagerFactory}
	 * @param name the name of the report
	 */
	private RedisReport(final DocumentFactory factory, final ConnectionManager connectionManager, final String name,
	                   final Charset charset) {
		super(new ReportCodec(factory, charset), new CommandSyncService(connectionManager), null == name ?
				DEFAULT_NAME : name);
		this.connectionManager = connectionManager;
	}

	/**
	 * Specifies that {@link RedisOutOfMemoryException} exceptions should trigger journaling of arguments when caught.
	 *
	 * @return a collection with only one object, the {@link RedisOutOfMemoryException} class
	 */
	public Optional<Collection<Class<? extends Exception>>> journalableExceptions() {
		return Optional.of(Collections.singletonList(RedisOutOfMemoryException.class));
	}

	@Override
	public void close() throws IOException {
		connectionManager.shutdown();
	}

	/**
	 * Codec for a map of string keys to integer values.
	 *
	 * @author Matthew Caruana Galizia <mcaruana@icij.org>
	 * @since 1.0.0-beta
	 */
	static class ReportCodec implements Codec {

		private final Decoder<Object> documentDecoder;
		private final Encoder documentEncoder;
		private final Decoder<Object> resultDecoder = new ResultDecoder();
		private final Encoder resultEncoder = new ResultEncoder();

		ReportCodec(final DocumentFactory factory, final Charset charset) {
			this.documentDecoder = new DocumentDecoder(factory, charset);
			this.documentEncoder = new DocumentEncoder(charset);
		}

		@Override
		public Decoder<Object> getMapKeyDecoder() {
			return documentDecoder;
		}

	    @Override
	    public Decoder<Object> getMapValueDecoder() {
			return resultDecoder;
	    }

	    @Override
	    public Encoder getMapValueEncoder() {
			return resultEncoder;
	    }

	    @Override
		public Encoder getMapKeyEncoder() {
			return documentEncoder;
	    }

	    @Override
		public Decoder<Object> getValueDecoder() {
			return resultDecoder;
	    }

	    @Override
		public Encoder getValueEncoder() {
			return resultEncoder;
	    }
	}
}
