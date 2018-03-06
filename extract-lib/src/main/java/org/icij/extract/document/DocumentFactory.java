package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Optional;

/**
 * A factory class for creating {@link TikaDocument} objects with default parameters.
 *
 * {@link org.icij.extract.queue.DocumentQueue} implementations should use the {@literal create} method that
 * instantiates a {@link TikaDocument} with all of the information that it is capable of providing.
 *
 * For example, a queue that stores only paths should use the {@link #create(Path)} method, whereas a queue that
 * stores both a path and ID should use {@link #create(String, Path)}.
 */
@Option(name = "idMethod", description = "The method for determining document IDs, for queues that use them. " +
		"Defaults to using the path as an ID.", parameter = "name")
@Option(name = "idDigestMethod", description = "For calculating document ID digests, where applicable depending on " +
		"the ID method.", parameter = "name")
@Option(name = "charset", description = "Set the output encoding for text and document attributes. Defaults to UTF-8.",
		parameter = "name")
public class DocumentFactory {

	private Identifier identifier = null;

	public DocumentFactory(final Options<String> options) {
		configure(options);
	}

	public DocumentFactory() {

	}

	public DocumentFactory configure(final Options<String> options) {
		final String algorithm = options.valueIfPresent("idDigestMethod").orElse("SHA-256");
		final Charset charset = Charset.forName(options.valueIfPresent("charset").orElse("UTF-8"));
		final Optional<String> method = options.valueIfPresent("idMethod");

		if (method.isPresent()) {
			switch (method.get()) {
				case "path":
					this.identifier = new PathIdentifier();
					break;
				case "path-digest":
					this.identifier = new PathDigestIdentifier(algorithm, charset);
					break;
				case "tika-digest":
					this.identifier = new DigestIdentifier(algorithm, charset);
					break;
				default:
					throw new IllegalArgumentException(String.format("\"%s\" is not a valid identifier.", method.get()));
			}
		} else {
			identifier = new DigestIdentifier(algorithm, charset);
		}

		return this;
	}

	public DocumentFactory withIdentifier(final Identifier identifier) {
		Objects.requireNonNull(identifier, "Identifier generator must not be null.");
		this.identifier = identifier;
		return this;
	}

	public TikaDocument create(final String id, final Path path) {
		return new TikaDocument(id, identifier, path);
	}

	public TikaDocument create(final String id, final Path path, final long size) {
		final Metadata metadata = new Metadata();

		metadata.set(Metadata.CONTENT_LENGTH, Long.toString(size));
		return new TikaDocument(id, identifier, path, metadata);
	}

	public TikaDocument create(final String id, final Path path, final Metadata metadata) {
		return new TikaDocument(id, identifier, path, metadata);
	}

	public TikaDocument create(final Path path) {
		return new TikaDocument(identifier, path);
	}

	public TikaDocument create(final Path path, final BasicFileAttributes attributes) {
		return create(path, attributes.size());
	}

	public TikaDocument create(final Path path, final long size) {
		final Metadata metadata = new Metadata();

		metadata.set(Metadata.CONTENT_LENGTH, Long.toString(size));
		return new TikaDocument(identifier, path, metadata);
	}

	public TikaDocument create(final String path) {
		return create(Paths.get(path));
	}

	public TikaDocument create(final String id, final String path) {
		return create(id, Paths.get(path));
	}

	public TikaDocument create(final Path path, final Metadata metadata) {
		return new TikaDocument(identifier, path, metadata);
	}

	public TikaDocument create(final URL url) throws URISyntaxException {
		return create(Paths.get(url.toURI()));
	}
}
