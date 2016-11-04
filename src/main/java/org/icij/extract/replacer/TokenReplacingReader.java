/*
 * Copyright 2011-2015 PrimeFaces Extensions
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.icij.extract.replacer;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

/**
 * Reader for in-place token replacements. It does not use as much memory as the {@link String#replace} method.
 *
 * Based on original code by Oleg Varaksin (ovaraksin@googlemail.com), the license of which is copied above. This
 * version resolves tokens to {@link java.io.InputStream} instances.
 */
public class TokenReplacingReader extends Reader {

	private final String start;
	private final String end;
	private final char[] startChars;
	private final char[] endChars;
	private char[] startCharsBuffer;
	private char[] endCharsBuffer;
	private final PushbackReader pushback;
	private final TokenResolver resolver;
	private final StringBuilder token = new StringBuilder();
	private Reader resolved = null;

	public TokenReplacingReader(final TokenResolver resolver, final Reader source, final String start, final String end) {
		if (resolver == null) {
			throw new IllegalArgumentException("Token resolver is null");
		}

		if ((start == null || start.length() < 1) || (end == null || end.length() < 1)) {
			throw new IllegalArgumentException("Token start / end marker is null or empty");
		}

		this.start = start;
		this.end = end;
		this.startChars = start.toCharArray();
		this.endChars = end.toCharArray();
		this.startCharsBuffer = new char[start.length()];
		this.endCharsBuffer = new char[end.length()];
		this.pushback = new PushbackReader(source, Math.max(start.length(), end.length()));
		this.resolver = resolver;
	}

	@Override
	public int read() throws IOException {
		if (resolved != null) {
			final int c = resolved.read();

			if (-1 == c) {
				resolved.close();
				resolved = null;
			} else {
				return c;
			}
		}

		// Read the proper number of chars into a temporary character array in order
		// to find token start marker.
		int countValidChars = readChars(startCharsBuffer);

		if (!Arrays.equals(startCharsBuffer, startChars)) {
			if (countValidChars > 0) {
				pushback.unread(startCharsBuffer, 0, countValidChars);
			}

			return pushback.read();
		}

		// Found start of token, read proper number of characters into a temporary
		// character array in order to find token end marker.
		boolean endOfSource = false;
		token.delete(0, token.length());
		countValidChars = readChars(endCharsBuffer);

		while (!Arrays.equals(endCharsBuffer, endChars)) {
			if (countValidChars == -1) {

				// End of source and no token end marker was found.
				endOfSource = true;
				break;
			}

			token.append(endCharsBuffer[0]);

			pushback.unread(endCharsBuffer, 0, countValidChars);
			if (pushback.read() == -1) {

				// End of source and no token end marker was found.
				endOfSource = true;
				break;
			}

			countValidChars = readChars(endCharsBuffer);
		}

		if (endOfSource) {
			resolved = new StringReader(start + token.toString());
		} else {

			// Try to resolve the token.
			resolved = resolver.resolveToken(token.toString());
			if (resolved == null) {

				// Token was not resolved.
				resolved = new StringReader(start + token.toString() + end);
			}
		}

		return resolved.read();
	}

	private int readChars(char[] tmpChars) throws IOException {
		int countValidChars = -1;
		int length = tmpChars.length;
		int data = pushback.read();

		for (int i = 0; i < length; i++) {
			if (data != -1) {
				tmpChars[i] = (char) data;
				countValidChars = i + 1;
				if (i + 1 < length) {
					data = pushback.read();
				}
			} else {

				// Reset to java default value for char.
				tmpChars[i] = '\u0000';
			}
		}

		return countValidChars;
	}

	@Override
	public int read(char[] buffer) throws IOException {
		return read(buffer, 0, buffer.length);
	}

	@Override
	public int read(char[] buffer, int offset, int length) throws IOException {
		int read = 0;

		for (int i = 0; i < length; i++) {
			int c = read();

			if (c == -1) {
				read = i;
				if (read == 0) {
					read = -1;
				}

				break;
			} else {
				read = i + 1;
			}

			buffer[offset + i] = (char) c;
		}

		return read;
	}

	@Override
	public void close() throws IOException {
		IOException thrown = null;

		try {
			pushback.close();
		} catch (IOException e) {
			thrown = e;
		}

		if (null != resolved) {
			try {
				resolved.close();
			} catch (IOException e) {
				thrown = e;
			}
		}

		if (null != thrown) {
			throw thrown;
		}
	}

	@Override
	public boolean ready() throws IOException {
		return pushback.ready();
	}

	@Override
	public boolean markSupported() {
		return pushback.markSupported();
	}

	@Override
	public long skip(long n) throws IOException {
		return pushback.skip(n);
	}

	@Override
	public void mark(int readAheadLimit) throws IOException {
		pushback.mark(readAheadLimit);
	}

	@Override
	public void reset() throws IOException {
		pushback.reset();
	}
}
