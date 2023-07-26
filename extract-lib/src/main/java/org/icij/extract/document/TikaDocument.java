package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY;

public class TikaDocument {
	public static String CONTENT_ENCODING = "Content-Encoding";
	public static String CONTENT_LANGUAGE = "Content-Language";
	public static String CONTENT_LENGTH = "Content-Length";
	public static String CONTENT_LOCATION = "Content-Location";
	public static String CONTENT_DISPOSITION = "Content-Disposition";
	public static String CONTENT_MD5 = "Content-MD5";
	public static String CONTENT_TYPE = "Content-Type";
	public static Property LAST_MODIFIED = Property.internalDate("Last-Modified");
	public static String LOCATION = "Location";

	private final Path path;
	private Supplier<String> id;
	private String language = null;
	private String foreignId = null;
	private final Metadata metadata;
	private Identifier identifier;
	private List<EmbeddedTikaDocument> embeds = new LinkedList<>();
	private Map<String, EmbeddedTikaDocument> lookup = new HashMap<>();
	private Reader reader = null;
	private ReaderGenerator readerGenerator = null;

	/**
	 * Instantiate a document with a pre-generated ID. In this case, the ID generator is only used when adding
	 * embedded documents to this parent.
	 *
	 * @param id a pre-generated ID
	 * @param identifier an identifier generator
	 * @param path the path to the document
	 * @param language the language of the document
	 * @param metadata document metadata
	 */
	public TikaDocument(final String id, final Identifier identifier, final Path path, final String language,  final Metadata metadata) {
		Objects.requireNonNull(path, "The path must not be null.");

		this.metadata = metadata;
		this.path = path;
		this.identifier = identifier;
		this.language = language;
		this.id = ()-> id;
	}

	/**
	 * Instantiate a document with a pre-generated ID. In this case, the ID generator is only used when adding
	 * embedded documents to this parent.
	 *
	 * @param id a pre-generated ID
	 * @param identifier an identifier generator
	 * @param path the path to the document
	 * @param metadata document metadata
	 */
	public TikaDocument(final String id, final Identifier identifier, final Path path, final Metadata metadata) {
		Objects.requireNonNull(path, "The path must not be null.");

		this.metadata = metadata;
		this.path = path;
		this.identifier = identifier;
		this.id = ()-> id;
	}

	/**
	 * @see TikaDocument (String, Identifier, Path, String, Metadata)
	 */
	public TikaDocument(final String id, final Identifier identifier, final Path path) {
		this(id, identifier, path, new Metadata());
	}

	/**
	 * @see TikaDocument (String, Identifier, Path, String, Metadata)
	 */
	public TikaDocument(final String id, final Identifier identifier, final Path path, final String language) {
		this(id, identifier, path, language, new Metadata());
	}

	/**
	 * Instantiate a document when the ID has not yet been generated.
	 *
	 * @param identifier for generating the ID
	 * @param path the path to the document
	 * @param metadata document metadata
	 */
	public TikaDocument(final Identifier identifier, final Path path, final Metadata metadata) {
		Objects.requireNonNull(identifier, "The identifier generator must not be null.");
		Objects.requireNonNull(path, "The path must not be null.");

		if (metadata.get(RESOURCE_NAME_KEY) == null) {
			metadata.add(RESOURCE_NAME_KEY, path.toFile().getName());
		}

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
	 * Instantiate a document when the ID has not yet been generated.
	 *
	 * @param identifier for generating the ID
	 * @param path the path to the document
	 * @param language the language of the document
	 * @param metadata document metadata
	 */
	public TikaDocument(final Identifier identifier, final Path path, final String language, final Metadata metadata) {
		this(identifier, path, metadata);
		this.language = language;
	}

	/**
	 * @see TikaDocument (Identifier, Path, Metadata)
	 */
	public TikaDocument(final Identifier identifier, final Path path) {
		this(identifier, path, new Metadata());
	}

	/**
	 * @see TikaDocument (Identifier, Path,  language, Metadata)
	 */
	public TikaDocument(final Identifier identifier, final Path path, final String language) {
		this(identifier, path, language, new Metadata());
	}

	String generateId() throws Exception {
		return identifier.generate(this);
	}

	public String getId() {
		return id.get();
	}

	public String getLanguage() {
		return language;
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

	public String getMetadata(final String fieldName) {
		return metadata.get(fieldName);
	}

	public Metadata getMetadata() {
		return metadata;
	}

	public EmbeddedTikaDocument addEmbed(final Metadata metadata) {
		return addEmbed(new EmbeddedTikaDocument(this, metadata));
	}

	private EmbeddedTikaDocument addEmbed(final Identifier identifier, final Path path, final Metadata metadata) {
		return addEmbed(new EmbeddedTikaDocument(this, identifier, path, metadata));
	}

	public EmbeddedTikaDocument addEmbed(final String key, final Identifier identifier, final Path path, final Metadata
			metadata) {
		return lookup.put(key, addEmbed(identifier, path, metadata));
	}

	private EmbeddedTikaDocument addEmbed(final EmbeddedTikaDocument embed) {
		embeds.add(embed);
		return embed;
	}

	public boolean removeEmbed(final EmbeddedTikaDocument embed) {
		return embeds.remove(embed);
	}

	public List<EmbeddedTikaDocument> getEmbeds() {
		return embeds;
	}

	public boolean hasEmbeds() {
		return !embeds.isEmpty();
	}

	public EmbeddedTikaDocument getEmbed(final String key) {
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
		if (!(other instanceof TikaDocument)) {
			return false;
		}

		// Only documents with the same ID are equal, as paths are not globally unique unless explicitly declared so,
		// if, for example, the PathIdentifier is used.
		final String id = getId();
		return null != id && id.equals(((TikaDocument) other).getId());
	}

	@Override
	public int hashCode() { return Objects.hash(id.get());}

	public void apply(Consumer<TikaDocument> consumer) {
		consumer.accept(this);
		for (EmbeddedTikaDocument doc: getEmbeds()) {
			doc.apply(consumer);
		}
  	}

	@Override
	public String toString() {
		return path.toString() + " - " + getMetadata("resourceName");
	}

	@FunctionalInterface
	public interface ReaderGenerator { Reader generate() throws IOException;}
}
