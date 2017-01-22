package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.TikaMetadataKeys;

import javax.xml.bind.DatatypeConverter;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static java.util.Locale.ENGLISH;

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

		if (document instanceof EmbeddedDocument) {
			return generateEmbedded((EmbeddedDocument) document, hash);
		}

		return hash;
	}

	private String generateEmbedded(final EmbeddedDocument embed, final String hash) {
		final MessageDigest digest;

		try {
			digest = MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		// Embedded documents in different files or the same file could have the same hash. Add the parent path to the
		// value to be digested, the embedded relationship ID, and the actual hash.
		final String embeddedRelationshipId = embed.getMetadata().get(TikaMetadataKeys.EMBEDDED_RELATIONSHIP_ID);

		digest.update(hash.getBytes(UTF_8));
		digest.update(embed.getParent().getId().getBytes(UTF_8));
		if (null != embeddedRelationshipId) {
			digest.update(embeddedRelationshipId.getBytes(UTF_8));
		}

		return DatatypeConverter.printHexBinary(digest.digest()).toLowerCase(ENGLISH);
	}
}
