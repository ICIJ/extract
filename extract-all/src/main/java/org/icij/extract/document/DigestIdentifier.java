package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

import javax.xml.bind.DatatypeConverter;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static java.util.Locale.ENGLISH;

public class DigestIdentifier extends AbstractIdentifier {

	DigestIdentifier(final String algorithm, final Charset charset) {
		super(algorithm, charset);
	}

	@Override
	public String generate(final Document document) {
		return hash(document).toLowerCase(ENGLISH);
	}

	@Override
	public String generateForEmbed(final EmbeddedDocument embed) throws NoSuchAlgorithmException {
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
		final String hash = hash(embed);

		if (null == hash) {
			throw new IllegalStateException(String.format("No hash is available for the document with name \"%s\" at " +
							"path \"%s\".", name, embed.getPath()));
		}

		digest.update(hash.getBytes(charset));
		digest.update(embed.getParent().getId().getBytes(charset));

		if (null != embeddedRelationshipId) {
			digest.update(embeddedRelationshipId.getBytes(charset));
		}

		if (null != name) {
			digest.update(name.getBytes(charset));
		}

		return DatatypeConverter.printHexBinary(digest.digest()).toLowerCase(ENGLISH);
	}
}
