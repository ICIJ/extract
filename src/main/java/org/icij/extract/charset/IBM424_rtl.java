package org.icij.extract.charset;

import java.util.Map;
import java.util.Iterator;
import java.util.TreeMap;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.spi.CharsetProvider;

/**
 * Creates a "IBM424_rtl" alias for the IBM424 character set.
 *
 * Temporary shim for TIKA-1236 until that issue is resolved.
 *
 * @since 1.0.0-beta
 */
public class IBM424_rtl extends CharsetProvider {

	private final Map<String, Charset> map = new TreeMap<>();

	public IBM424_rtl() {
		map.put("IBM424_rtl", new IBM424_rtlCharset());
	}

	@Override
	public Iterator<Charset> charsets() {
		return map.values().iterator();
	}

	@Override
	public Charset charsetForName(String name) {
		return map.get(name);
	}

	private static class IBM424_rtlCharset extends Charset {
		private static final Charset target = Charset.forName("IBM424");

		public IBM424_rtlCharset() {
			super("IBM424_rtl", new String[0]);
		}

		@Override
		public boolean contains(Charset cs) {
			return target.contains(cs);
		}
		
		@Override
		public CharsetDecoder newDecoder() {
			return target.newDecoder();
		}
		
		@Override
		public CharsetEncoder newEncoder() {
			return target.newEncoder();
		}
	}
}
