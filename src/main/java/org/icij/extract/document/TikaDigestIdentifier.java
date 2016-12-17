package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

public class TikaDigestIdentifier implements Identifier {

	private final String key;
	private final String algorithm;

	TikaDigestIdentifier(final String algorithm) {
		key = TikaCoreProperties.TIKA_META_PREFIX + "digest" + Metadata.NAMESPACE_PREFIX_DELIMITER + algorithm
				.replace("-", "");
		this.algorithm = algorithm;
	}

	@Override
	public String generate(final Document document) {
		final String hash = document.getMetadata().get(key);

		if (null == hash) {
			throw new RuntimeException("Unexpected null hash. Check that the correct algorithm is specified.");
		}

		if (!(document instanceof EmbeddedDocument)) {
			return hash;
		}

		final MessageDigest digest;

		try {
			digest = MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		digest.update(((EmbeddedDocument) document).getParent().getId().getBytes(StandardCharsets.UTF_8));
		digest.update(hash.getBytes(StandardCharsets.UTF_8));

		return DatatypeConverter.printHexBinary(digest.digest()).toLowerCase(Locale.ROOT);
	}
}
