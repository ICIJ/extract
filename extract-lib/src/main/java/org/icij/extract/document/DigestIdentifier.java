package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static java.util.Locale.ENGLISH;

public class DigestIdentifier extends AbstractIdentifier {

	public DigestIdentifier(final String algorithm, final Charset charset) {
		super(algorithm, charset);
	}

	@Override
	public String generate(final TikaDocument tikaDocument) {
		return hash(tikaDocument).toLowerCase(ENGLISH);
	}

	@Override
	public String generateForEmbed(final EmbeddedTikaDocument embed) throws NoSuchAlgorithmException {
		final MessageDigest digest = MessageDigest.getInstance(algorithm);

		// Embedded documents in different files or the same file could have the same hash. Therefore, to avoid ID
		// collisions within the child document tree, the digest considers:
		// - the file digest hash
		// - the parent path
		// - the embedded relationship ID
		// - the embedded document name
		final Metadata metadata = embed.getMetadata();
		final String embeddedRelationshipId = metadata.get(TikaCoreProperties.EMBEDDED_RELATIONSHIP_ID);
		final String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
		final String hash = hash(embed);

		// A content-less embed has no file digest. This happens for a PST/OST by-value attachment the
		// reader could not read (Tika emits a name-only embed) and for attachment-recovery stubs.
		// Rather than fail the whole extraction, derive the id from the parent id, embedded
		// relationship id and name, which still disambiguate siblings; only the file-digest component
		// is dropped. When a hash is present the id is unchanged (same components, same order).
		if (null != hash) {
			digest.update(hash.getBytes(charset));
		} else {
			LoggerFactory.getLogger(getClass()).debug(
					"No content hash for embed \"{}\" (relId {}) under parent {}; deriving id without the file digest.",
					name, embeddedRelationshipId, embed.getParent().getId());
		}

		digest.update(embed.getParent().getId().getBytes(charset));

		if (null != embeddedRelationshipId) {
			digest.update(embeddedRelationshipId.getBytes(charset));
		}

		if (null != name) {
			digest.update(name.getBytes(charset));
		}

		String parentId = embed.getParent().getId();
		String lowerCase = DatatypeConverter.printHexBinary(digest.digest()).toLowerCase(ENGLISH);
		LoggerFactory.getLogger(getClass()).debug(
				"hash={} name={} relId={} parentId={} => {}", hash, name, embeddedRelationshipId, parentId, lowerCase);
		return lowerCase;
	}
}
