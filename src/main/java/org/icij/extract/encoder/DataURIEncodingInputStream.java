package org.icij.extract.encoder;

import java.util.Map;

import java.io.Reader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedInputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.AutoDetectReader;
import org.apache.tika.exception.TikaException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.codec.binary.Base64InputStream;

/**
 * An {@link InputStream} that encodes arbitrary binary data from an
 * input stream in data URI format.
 *
 * This object is thread-safe.
 *
 * @since 1.0.0-beta
 */
public class DataURIEncodingInputStream extends InputStream {

	protected static MediaType detectType(final Path path, final Metadata metadata) throws IOException {
		MediaType type = null;

		// There seems to be some confusion in Tika about which key to use.
		String orig = metadata.get(Metadata.CONTENT_TYPE);
		if (null == orig || orig.isEmpty()) {
			orig = metadata.get(TikaCoreProperties.TYPE);
		}

		if (null != orig) {
			type = MediaType.parse(orig);
		}

		if (null == type) {
			try (
				final InputStream input = new BufferedInputStream(Files.newInputStream(path));
			) {
				type = new DefaultDetector().detect(input, metadata);
			} catch (IOException e) {
				throw e;
			}
		}

		// If the type is text, detect the charset if it's missing from
		// the mediatype and add it to it as a param.
		if (type.getType().equals("text")) {
			final Map<String, String> parameters = type.getParameters();

			if (null != parameters.get("charset")) {
				return type;
			}

			final Charset charset = detectCharset(path, metadata);

			if (null != charset) {
				parameters.put("charset", charset.name());
				type = new MediaType(type.getBaseType(), parameters);
			}
		}

		return type;
	}

	protected static Charset detectCharset(final Path path, final Metadata metadata) throws IOException {
		Charset charset = null;

		// Try to get the character set from the content-encoding.
		String orig = metadata.get(Metadata.CONTENT_ENCODING);

		// Try to detect the character set.
		if (null != orig && Charset.isSupported(orig)) {
			return Charset.forName(orig);
		}

		try (
			final InputStream input = new BufferedInputStream(Files.newInputStream(path));
			final AutoDetectReader detector = new AutoDetectReader(input, metadata);
		) {
			charset = detector.getCharset();
		} catch (TikaException e) {
			throw new IOException("Unable to detect charset.", e);
		} catch (IOException e) {
			throw e;
		}

		return charset;
	}

	public static Reader createReader(final Path path, final Metadata metadata) throws IOException {
		return new InputStreamReader(new DataURIEncodingInputStream(path, metadata), StandardCharsets.US_ASCII);
	}

	private final InputStream encoder;
	private final byte[] prepend;
	private int position = 0;

	public DataURIEncodingInputStream(final Path path, final Metadata metadata) throws IOException {
		this(new BufferedInputStream(Files.newInputStream(path)), detectType(path, metadata));
	}

	public DataURIEncodingInputStream(final InputStream in, final MediaType type) {

		// Only text documents should be URL-encoded. It doesn't matter if the encoding
		// is supported or not because the URL-encoder works on raw bytes.
		// Everything else must be base-64-encoded.
		if (type.getType().equals("text")) {
			this.prepend = ("data:" + type + ",").getBytes(StandardCharsets.US_ASCII);
			this.encoder = new URLEncodingInputStream(in);
		} else {
			this.prepend = ("data:" + type + ";base64,").getBytes(StandardCharsets.US_ASCII);
			this.encoder = new Base64InputStream(in, true, 76, "\n".getBytes(StandardCharsets.US_ASCII));
		}
	}

	@Override
	public int read() throws IOException {
		if (position < prepend.length) {
			return prepend[position++];
		} else {
			return encoder.read();
		}
	}

	@Override
	public int available() throws IOException {
		if (position < prepend.length) {
			return prepend.length - position;
		} else {
			return encoder.available();
		}
	}

	@Override
	public void close() throws IOException {
		encoder.close();
	}
}
