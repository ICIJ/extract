package org.icij.extract.redis;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.queue.DocumentQueue;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.redisson.Redisson;
import org.redisson.RedissonBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandSyncService;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * A {@link DocumentQueue} using Redis as a backend.
 *
 *
 */
@Option(name = "queueName", description = "The name of the queue.", parameter = "name")
@Option(name = "charset", description = "Set the output encoding for strings. Defaults to UTF-8.", parameter = "name")
@OptionsClass(RedissonClientFactory.class)
public class RedisDocumentQueue extends RedissonBlockingQueue<TikaDocument> implements DocumentQueue {
	/**
	 * The default name for a queue in Redis.
	 */
	private static final String DEFAULT_NAME = "extract:queue";

	private final RedissonClient redissonClient;

	/**
	 * Create a Redis-backed queue using the provided configuration.
	 *
	 * @param factory for creating {@link TikaDocument} objects
	 * @param options options for connecting to Redis
	 */
	public RedisDocumentQueue(final DocumentFactory factory, final Options<String> options) {
		this(factory, new RedissonClientFactory().withOptions(options).create(),
				options.valueIfPresent("queueName").orElse(DEFAULT_NAME),
				Charset.forName(options.valueIfPresent("charset").orElse("UTF-8")));
	}

	/**
	 * Instantiate a new Redis-backed queue using the provided connection manager and name.
	 *
	 * @param factory for creating {@link TikaDocument} objects
	 * @param redissonClient instantiated using {@link RedissonClientFactory}
	 * @param name the name of the queue
	 * @param charset the character set for encoding and decoding paths
	 */
	private RedisDocumentQueue(final DocumentFactory factory, final RedissonClient redissonClient,
	                           final String name, final Charset charset) {
		this(new DocumentQueueCodec(factory, charset),
				new CommandSyncService(((Redisson)redissonClient).getConnectionManager()),
				null == name ? DEFAULT_NAME : name, redissonClient);

	}

	private RedisDocumentQueue(Codec codec, CommandAsyncExecutor commandExecutor, String name, RedissonClient redisson) {
		super(codec, commandExecutor, name, redisson);
		this.redissonClient = redisson;
	}

	@Override
	public DocumentQueue newQueue() {
		return new RedisDocumentQueue(codec, new CommandSyncService(((Redisson)redissonClient).getConnectionManager()),
				getName()+ ":filtered", redissonClient);
	}

	@Override
	public void close() throws IOException {
		redissonClient.shutdown();
	}

	/**
	 * Codec for a queue of paths to documents.
	 */
	static class DocumentQueueCodec extends BaseCodec {

		private final Decoder<Object> documentDecoder;
		private final Encoder documentEncoder;

		DocumentQueueCodec(final DocumentFactory factory, final Charset charset) {
			documentDecoder = new DocumentDecoder(factory, charset);
			documentEncoder = new DocumentEncoder(charset);
		}

		@Override
		public Decoder<Object> getValueDecoder() {
			return documentDecoder;
		}

		@Override
		public Decoder<Object> getMapValueDecoder() {
			return documentDecoder;
		}

		@Override
		public Decoder<Object> getMapKeyDecoder() {
			return documentDecoder;
		}

		@Override
		public Encoder getMapValueEncoder() {
			return documentEncoder;
		}

		@Override
		public Encoder getMapKeyEncoder() {
			return documentEncoder;
		}

		@Override
		public Encoder getValueEncoder() {
			return documentEncoder;
		}
	}
}
