package org.icij.extract.document;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.util.Locale.ENGLISH;

public class PathDigestIdentifier implements Identifier {

	private final String algorithm;
	private final Charset charset;

	PathDigestIdentifier(final String algorithm, final Charset charset) {
		this.algorithm = algorithm;
		this.charset = charset;
	}

	@Override
	public String generate(final Document document) throws NoSuchAlgorithmException {
		return DatatypeConverter.printHexBinary(MessageDigest.getInstance(algorithm).digest(document.getPath()
				.toString().getBytes(charset))).toLowerCase(ENGLISH);
	}
}
