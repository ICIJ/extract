package org.icij.extract.redis;

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
import org.redisson.liveobject.core.RedissonObjectBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.HashMap;

/**
 * A {@link DocumentQueue} using Redis as a backend.
 *
 *
 */
@Option(name = "queueName", description = "The name of the queue.", parameter = "name")
@Option(name = "charset", description = "Set the output encoding for strings. Defaults to UTF-8.", parameter = "name")
@OptionsClass(RedissonClientFactory.class)
public class RedisDocumentQueue<T> extends RedissonBlockingQueue<T> implements DocumentQueue<T> {
	/**
	 * The default name for a queue in Redis.
	 */
	private static final String DEFAULT_NAME = "extract:queue";

	private final RedissonClient redissonClient;

	/**
	 * Create a Redis-backed queue with path identifier document factory
	 *
	 * @param queueName name of the redis key
	 * @param redisAddress redis url i.e. redis://127.0.0.1:6379
	 */
	public RedisDocumentQueue(final String queueName, final String redisAddress, final Class<T> clazz) {
		this(Options.from(new HashMap<>() {{
            put("redisAddress", redisAddress);
            put("queueName", queueName);
        }}), clazz);
	}

	/**
	 * Create a Redis-backed queue using the provided configuration.
	 *
	 * @param options options for connecting to Redis
	 */
	public RedisDocumentQueue(final Options<String> options, Class<T> clazz) {
		this(new RedissonClientFactory().withOptions(options).create(),
				options.valueIfPresent("queueName").orElse(DEFAULT_NAME),
				Charset.forName(options.valueIfPresent("charset").orElse("UTF-8")), clazz);
	}

	/**
	 * Instantiate a new Redis-backed queue using the provided connection manager and name.
	 *
	 * @param redissonClient instantiated using {@link RedissonClientFactory}
	 * @param name the name of the queue
	 * @param charset the character set for encoding and decoding paths
	 */
	private RedisDocumentQueue(final RedissonClient redissonClient,
	                           final String name, final Charset charset, final Class<T> clazz) {
		this(new QueueCodec<>(charset, clazz),
				new CommandSyncService(((Redisson)redissonClient).getConnectionManager(), new RedissonObjectBuilder(redissonClient)),
				null == name ? DEFAULT_NAME : name, redissonClient);

	}

	private RedisDocumentQueue(Codec codec, CommandAsyncExecutor commandExecutor, String name, RedissonClient redisson) {
		super(codec, commandExecutor, name, redisson);
		this.redissonClient = redisson;
	}

	@Override
	public boolean remove(Object o, int count) {
		return super.remove(o, count);
	}

	@Override
	public void close() throws IOException {
		redissonClient.shutdown();
	}

	@Override
	public String toString() {
		return "RedisDocumentQueue{name=" + getName() + '}';
	}


	/**
	 * Codec for a queue of paths to documents.
	 */
	static class QueueCodec<T> extends BaseCodec {

		private final Decoder<Object> decoder;
		private final Encoder documentEncoder;

		QueueCodec(final Charset charset, Class<T> clazz) {
			decoder = clazz.isAssignableFrom(Path.class) ?
					new PathDecoder(charset):
					(buf, state) -> {
						String str = buf.toString(charset);
						buf.readerIndex(buf.readableBytes());
						return str;
					};
            documentEncoder = new PathEncoder(charset);
		}

		@Override
		public Decoder<Object> getValueDecoder() {
			return decoder;
		}

		@Override
		public Decoder<Object> getMapValueDecoder() {
			return decoder;
		}

		@Override
		public Decoder<Object> getMapKeyDecoder() {
			return decoder;
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
