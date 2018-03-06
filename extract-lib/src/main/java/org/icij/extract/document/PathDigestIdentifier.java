package org.icij.extract.document;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.util.Locale.ENGLISH;

public class PathDigestIdentifier extends AbstractIdentifier {

	PathDigestIdentifier(final String algorithm, final Charset charset) {
		super(algorithm, charset);
	}

	@Override
	public String generate(final Document document) throws NoSuchAlgorithmException {
		final MessageDigest digest = MessageDigest.getInstance(algorithm);

		digest.update(document.getPath().toString().getBytes(charset));
		return DatatypeConverter.printHexBinary(digest.digest()).toLowerCase(ENGLISH);
	}

	@Override
	public String generateForEmbed(final EmbeddedDocument embed) throws NoSuchAlgorithmException {
		return generate(embed);
	}
}
