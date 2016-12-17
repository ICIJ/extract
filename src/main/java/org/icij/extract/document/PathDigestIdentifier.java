package org.icij.extract.document;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PathDigestIdentifier implements Identifier {

	private final String algorithm;
	private final Charset charset;

	PathDigestIdentifier(final String algorithm, final Charset charset) {
		this.algorithm = algorithm;
		this.charset = charset;
	}

	@Override
	public String generate(final Document document) {
		try {
			return DatatypeConverter.printHexBinary(MessageDigest.getInstance(algorithm)
					.digest(document.getPath().toString().getBytes(charset)));
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
