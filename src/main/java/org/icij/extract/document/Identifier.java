package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

/**
 * An {@linkplain Identifier} holds logic for generating both unique identifiers for documents as well as digest
 * hashes of the the underlying file data.
 */
public interface Identifier {

	/**
	 * Generate an identifier for a root document.
	 *
	 * @param document the document to generate an identifier for
	 * @return A unique identifier, for example a fixed-length hash.
	 * @throws Exception if there's an exception generating the ID
	 */
	String generate(final Document document) throws Exception;

	/**
	 * Generate an identifier for an embedded document.
	 *
	 * @param document the embedded document to generate an ID for
	 * @return A unique identifier for the embedded document.
	 * @throws Exception if there's an error generating the ID
	 */
	String generateForEmbed(final EmbeddedDocument document) throws Exception;

	/**
	 * Generate or retrieve (from metadata) a hash digest of the document's underlying file data.
	 *
	 * Even if the {@link #generate(Document)} methods of the implementation generate hash digests, those are
	 * semantically different as they represent a hash of the document, rather than the file. The former might
	 * comprise the the relationship of the document with its parent, or its position in the path hierarchy, whereas
	 * the latter must not.
	 *
	 * @param document the document for which to return a file hash digest
	 * @return the hash
	 * @throws Exception if there's an error generating the hash
	 */
	String hash(final Document document) throws Exception;

	/**
	 * Retrieve a hash digest of the document's underlying file data.
	 *
	 * @param metadata the document's metadata
	 * @return the hash
	 */
	String retrieveHash(final Metadata metadata);
}
