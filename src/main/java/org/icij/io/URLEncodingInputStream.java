package org.icij.io;

import java.io.Reader;
import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.nio.charset.StandardCharsets;

/**
 * A {@link FilterInputStream} that URL-encodes arbitrary binary data from an input stream.
 *
 * No transcoding is performed on the input. If your input is UTF-16LE, the output should be read as percent-encoded
 * UTF-16LE. If your input if UTF-8, the output will be percent-encoded UTF-8, and so on.
 *
 * This object is thread-safe.
 *
 * @since 1.0.0-beta
 */
public class URLEncodingInputStream extends FilterInputStream {

	private final int[] buffer = new int[3];
	private int offset = 0;

	public URLEncodingInputStream(final InputStream in) {
		super(in);
	}

	public static Reader reader(final InputStream in) {
		return new InputStreamReader(new URLEncodingInputStream(in), StandardCharsets.US_ASCII);
	}

	@Override
	public synchronized int read() throws IOException {
		if (offset < 3) {
			return buffer[offset++];
		}

		final int b = in.read();

		if (-1 == b) {
			return -1;
		}

		encode(b);
		return buffer[offset++];
	}

	@Override
	public int read(byte[] buffer) throws IOException {
		return read(buffer, 0, buffer.length);
	}

	@Override
	public synchronized int read(byte[] bbuf, int off, int len) throws IOException {
		int read = 0;

		while (read < len) {
			final int b = read();

			if (-1 == b) {
				break;
			} else {
				bbuf[read++] = (byte) b;
			}
		}

		if (0 == read) {
			return -1;
		} else {
			return read;
		}
	}

	private void encode(final int b) {
		if (' ' == b) {
			offset = 2;
			buffer[offset] = (int) '+';

		} else if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') ||
			'-' == b || '_' == b || '.' == b || '*' == b) {
			offset = 2;
			buffer[offset] = b;

		} else {
			offset = 0;
			buffer[0] = (int) '%';
			buffer[1] = b >> 4;
			buffer[2] = b & 0x0f;
		}
	}
}
