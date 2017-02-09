package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import javax.xml.bind.DatatypeConverter;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static java.util.Locale.ENGLISH;

public class DigestIdentifier implements Identifier {

	private final String key;
	private final String algorithm;
	private final Charset charset;

	DigestIdentifier(final String algorithm, final Charset charset) {
		key = TikaCoreProperties.TIKA_META_PREFIX + "digest" + Metadata.NAMESPACE_PREFIX_DELIMITER + algorithm
				.replace("-", "");
		this.algorithm = algorithm;
		this.charset = charset;
	}

	@Override
	public String generate(final Document document) throws NoSuchAlgorithmException {
		String hash = document.getMetadata().get(key);

		if (null == hash) {
			throw new RuntimeException("Unexpected null hash. Check that the correct algorithm is specified.");
		}

		if (document instanceof EmbeddedDocument) {
			hash = generateEmbedded((EmbeddedDocument) document, hash);
		}

		return hash.toLowerCase(ENGLISH);
	}

	private String generateEmbedded(final EmbeddedDocument embed, final String hash) throws NoSuchAlgorithmException {
		final MessageDigest digest = MessageDigest.getInstance(algorithm);

		// Embedded documents in different files or the same file could have the same hash. Therefore, to avoid ID
		// collisions within the child document tree, the digest considers:
		// - the file digest hash
		// - the parent path
		// - the embedded relationship ID
		// - the embedded document name
		final Metadata metadata = embed.getMetadata();
		final String embeddedRelationshipId = metadata.get(Metadata.EMBEDDED_RELATIONSHIP_ID);
		final String name = metadata.get(Metadata.RESOURCE_NAME_KEY);

		digest.update(hash.getBytes(charset));
		digest.update(embed.getParent().getId().getBytes(charset));

		if (null != embeddedRelationshipId) {
			digest.update(embeddedRelationshipId.getBytes(charset));
		}

		if (null != name) {
			digest.update(name.getBytes(charset));
		}

		return DatatypeConverter.printHexBinary(digest.digest());
	}
}
