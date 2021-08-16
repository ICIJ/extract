package org.icij.extract.redis;

import org.icij.extract.report.Report;
import org.icij.extract.report.ReportMap;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.redisson.Redisson;
import org.redisson.RedissonMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisOutOfMemoryException;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.command.CommandSyncService;
import org.redisson.connection.ConnectionManager;
import org.redisson.liveobject.core.RedissonObjectBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

/**
 * A {@link ReportMap} using Redis as a backend.
 *
 *
 */
@Option(name = "reportName", description = "The name of the report, the default of which is type-dependent.",
		parameter = "name")
@Option(name = "charset", description = "Set the output encoding for text and document attributes. Defaults to UTF-8.",
		parameter = "name")
@OptionsClass(ConnectionManager.class)
public class RedisReportMap extends RedissonMap<Path, Report> implements ReportMap {

	/**
	 * The default name for a report in Redis.
	 */
	private static final String DEFAULT_NAME = "extract:report";

	private final RedissonClient redissonClient;

	/**
	 *
	 * @param mapName name of the map in redis
	 * @param redisAddress redis url i.e. redis://127.0.0.1:6379
	 */
	public RedisReportMap(final String mapName, final String redisAddress) {
		this(Options.from(new HashMap<String, String>() {{
			put("reportName", mapName);
			put("redisAddress", redisAddress);
			put("charset", "UTF-8");
		}}));
	}

	/**
	 * Create a Redis-backed report using the provided configuration.
	 *
	 * @param options options for connecting to Redis
	 */
	public RedisReportMap(final Options<String> options) {
		this(new RedissonClientFactory().withOptions(options).create(),
				options.get("reportName").value().orElse(DEFAULT_NAME),
				options.get("charset").parse().asCharset().orElse(StandardCharsets.UTF_8));
	}

	/**
	 * Instantiate a new Redis-backed report using the provided connection manager and name.
	 *
	 * @param redissonClient instantiated using {@link RedissonClientFactory}
	 * @param name the name of the report
	 */
	private RedisReportMap(final RedissonClient redissonClient, final String name,
						   final Charset charset) {
		super(new ReportCodec(charset), new CommandSyncService(((Redisson)redissonClient).getConnectionManager(), new RedissonObjectBuilder(redissonClient)),
				null == name ? DEFAULT_NAME : name, redissonClient, null, null);
		this.redissonClient = redissonClient;
	}

	/**
	 * Specifies that {@link RedisOutOfMemoryException} exceptions should trigger journaling of arguments when caught.
	 *
	 * @return a collection with only one object, the {@link RedisOutOfMemoryException} class
	 */
	public Collection<Class<? extends Exception>> journalableExceptions() {
		return Collections.singletonList(RedisOutOfMemoryException.class);
	}

	@Override
	public void close() throws IOException {
		redissonClient.shutdown();
	}

	static class ReportCodec extends BaseCodec {
		private final Decoder<Object> documentDecoder;
		private final Encoder documentEncoder;
		private final Decoder<Object> resultDecoder = new ResultDecoder();
		private final Encoder resultEncoder = new ResultEncoder();

		ReportCodec(final Charset charset) {
			this.documentDecoder = new PathDecoder(charset);
			this.documentEncoder = new PathEncoder(charset);
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
