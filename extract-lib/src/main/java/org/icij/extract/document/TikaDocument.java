package org.icij.extract.document;

import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY;

public class TikaDocument {
	public static final String TIKA_VERSION = "Tika-Version";
	public static String CONTENT_ENCODING = "Content-Encoding";
	public static String CONTENT_LANGUAGE = "Content-Language";
	public static String CONTENT_LENGTH = "Content-Length";
	public static String CONTENT_LOCATION = "Content-Location";
	public static String CONTENT_DISPOSITION = "Content-Disposition";
	public static String CONTENT_MD5 = "Content-MD5";
	public static String CONTENT_TYPE = "Content-Type";
	public static Property LAST_MODIFIED = Property.internalDate("Last-Modified");
	public static String LOCATION = "Location";
	public static final Map<Date, String> TIKA_VERSION_RECORDS = new ConcurrentSkipListMap<>() {{
		put(Date.from(Instant.parse("1970-01-01T00:00:00Z")), 	"1.8.0" );
		put(Date.from(Instant.parse("2015-07-08T03:59:48Z")), 	"1.9.0" );
		put(Date.from(Instant.parse("2015-08-10T23:05:19Z")), 	"1.10.0" );
		put(Date.from(Instant.parse("2015-10-28T13:36:23Z")), 	"1.11.0" );
		put(Date.from(Instant.parse("2016-02-27T01:46:47Z")), 	"1.12.0" );
		put(Date.from(Instant.parse("2016-05-24T15:29:32Z")), 	"1.13.0" );
		put(Date.from(Instant.parse("2016-11-04T18:58:15Z")), 	"1.14rc1");
		put(Date.from(Instant.parse("2016-11-14T12:23:43Z")), 	"1.14.0" );
		put(Date.from(Instant.parse("2017-06-11T18:43:49Z")), 	"1.15.0" );
		put(Date.from(Instant.parse("2017-08-17T16:09:55Z")), 	"1.16.0" );
		put(Date.from(Instant.parse("2018-02-13T09:49:53Z")), 	"1.17.0" );
		put(Date.from(Instant.parse("2018-06-11T13:04:21Z")), 	"1.18.0" );
		put(Date.from(Instant.parse("2019-06-07T10:35:53Z")), 	"1.20.0" );
		put(Date.from(Instant.parse("2019-08-12T15:16:26Z")), 	"1.22.0" );
		put(Date.from(Instant.parse("2020-09-14T08:27:25Z")), 	"1.24.1" );
		put(Date.from(Instant.parse("2020-09-16T16:20:26Z")), 	"1.22.0" );
		put(Date.from(Instant.parse("2021-04-02T16:13:51Z")), 	"1.24.1" );
		put(Date.from(Instant.parse("2021-04-02T16:52:36Z")), 	"1.22.0" );
		put(Date.from(Instant.parse("2022-10-10T11:10:34Z")), 	"1.23.0" );
		put(Date.from(Instant.parse("2022-10-20T11:57:11Z")), 	"2.4.1");
		put(Date.from(Instant.parse("2025-03-12T09:58:47Z")), 	"2.9.3");
		put(Date.from(Instant.parse("2025-03-12T10:52:07Z")), 	"3.1.0");
	}};

	private final Path path;
	private Supplier<String> id;
	private String language = null;
	private String foreignId = null;
	private final Metadata metadata;
	private final Identifier identifier;
	private final List<EmbeddedTikaDocument> embeds = new LinkedList<>();
	private final Map<String, EmbeddedTikaDocument> lookup = new HashMap<>();
	private Reader reader = null;
	private ReaderGenerator readerGenerator = null;
	private boolean isDuplicate;

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
		this.metadata.set(TIKA_VERSION, Tika.getString());
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
		this(id, identifier, path, null, metadata);
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
		this.metadata.set(TIKA_VERSION, Tika.getString());
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

	public static String getTikaVersion(Date date) {
		Map.Entry<Date, String> previous = TIKA_VERSION_RECORDS.entrySet().iterator().next();
		for (Map.Entry<Date, String> entry: TIKA_VERSION_RECORDS.entrySet()) {
			if (entry.getKey().after(date)) {
				return versionWithPrefix(previous.getValue());
			}
			previous = entry;
		}
		return versionWithPrefix(previous.getValue());
	}

	private static String versionWithPrefix(String rawVersion) {
		return "Apache Tika " + rawVersion;
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

	public boolean isDuplicate() {
		return isDuplicate;
	}

	public void setDuplicate(boolean duplicate) {
		isDuplicate = duplicate;
	}

	@FunctionalInterface
	public interface ReaderGenerator { Reader generate() throws IOException;}
}
