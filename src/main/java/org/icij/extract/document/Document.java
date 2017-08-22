package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;

public class Document {

	private final Path path;
	private Supplier<String> id;
	private String foreignId = null;
	private final Metadata metadata;

	private Identifier identifier;
	private List<EmbeddedDocument> embeds = new LinkedList<>();
	private Map<String, EmbeddedDocument> lookup = new HashMap<>();

	private Reader reader = null;
	private ReaderGenerator readerGenerator = null;

	/**
	 * Instantiate a document with a pre-generated ID. In this case, the ID generator is only used when adding
	 * embedded documents to this parent.
	 *
	 * @param id a pre-generated ID
	 * @param identifier an identifier generator
	 * @param path the path to the document
	 * @param metadata document metadata
	 */
	public Document(final String id, final Identifier identifier, final Path path, final Metadata metadata) {
		Objects.requireNonNull(path, "The path must not be null.");

		this.metadata = metadata;
		this.path = path;
		this.identifier = identifier;
		this.id = ()-> id;
	}

	/**
	 * @see Document(String, Identifier, Path, Metadata)
	 */
	public Document(final String id, final Identifier identifier, final Path path) {
		this(id, identifier, path, new Metadata());
	}

	/**
	 * Instantiate a document when the ID has not yet been generated.
	 *
	 * @param identifier for generating the ID
	 * @param path the path to the document
	 * @param metadata document metadata
	 */
	public Document(final Identifier identifier, final Path path, final Metadata metadata) {
		Objects.requireNonNull(identifier, "The identifier generator must not be null.");
		Objects.requireNonNull(path, "The path must not be null.");

		this.metadata = metadata;
		this.path = path;
		this.identifier = identifier;

		// Create a supplier that will cache the result of the generator after the first invocation.
		this.id = ()-> {
			final String id;

			try {
				id = this.generateId();
			} catch (final Exception e) {
				throw new RuntimeException("Unable to generate document ID.", e);
			}

			this.id = ()-> id;
			return id;
		};
	}

	/**
	 * @see Document(Identifier, Path, Metadata)
	 */
	public Document(final Identifier identifier, final Path path) {
		this(identifier, path, new Metadata());
	}

	String generateId() throws Exception {
		return identifier.generate(this);
	}

	public String getId() {
		return id.get();
	}

	public String getHash() {
		return identifier.retrieveHash(getMetadata());
	}

	Identifier getIdentifier() {
		return identifier;
	}

	public Path getPath() {
		return path;
	}

	public Metadata getMetadata() {
		return metadata;
	}

	public EmbeddedDocument addEmbed(final Metadata metadata) {
		return addEmbed(new EmbeddedDocument(this, metadata));
	}

	private EmbeddedDocument addEmbed(final Identifier identifier, final Path path, final Metadata metadata) {
		return addEmbed(new EmbeddedDocument(this, identifier, path, metadata));
	}

	public EmbeddedDocument addEmbed(final String key, final Identifier identifier, final Path path, final Metadata
			metadata) {
		return lookup.put(key, addEmbed(identifier, path, metadata));
	}

	private EmbeddedDocument addEmbed(final EmbeddedDocument embed) {
		embeds.add(embed);
		return embed;
	}

	public boolean removeEmbed(final EmbeddedDocument embed) {
		return embeds.remove(embed);
	}

	public List<EmbeddedDocument> getEmbeds() {
		return embeds;
	}

	public boolean hasEmbeds() {
		return !embeds.isEmpty();
	}

	public EmbeddedDocument getEmbed(final String key) {
		return lookup.get(key);
	}

	public void setReader(final Reader reader) {
		this.reader = reader;
	}

	public void setReader(final ReaderGenerator readerGenerator) {
		this.readerGenerator = readerGenerator;
	}

	public void clearReader() {
		this.reader = null;
		this.readerGenerator = null;
	}

	public void setForeignId(final String foreignId) {
		this.foreignId = foreignId;
	}

	public String getForeignId() {
		return foreignId;
	}

	public synchronized Reader getReader() throws IOException {
		if (null == reader && null != readerGenerator) {
			reader = readerGenerator.generate();
		}

		return reader;
	}

	@Override
	public boolean equals(final Object other) {
		if (!(other instanceof Document)) {
			return false;
		}

		// Only documents with the same ID are equal, as paths are not globally unique unless explicitly declared so,
		// if, for example, the PathIdentifier is used.
		final String id = getId();
		return null != id && id.equals(((Document) other).getId());
	}

	@Override
	public String toString() {
		return path.toString();
	}

	@FunctionalInterface
	public interface ReaderGenerator {

		Reader generate() throws IOException;
	}
}
