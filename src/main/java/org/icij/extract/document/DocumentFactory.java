package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.icij.task.Options;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * A factory class for creating {@link Document} objects with default parameters.
 *
 * {@link org.icij.extract.queue.DocumentQueue} implementations should use the {@literal create} method that
 * instantiates a {@link Document} with all of the information that it is capable of providing.
 *
 * For example, a queue that stores only paths should use the {@link #create(Path)} method, whereas a queue that
 * stores both a path and ID should use {@link #create(String, Path)}.
 */
public class DocumentFactory {

	private Identifier identifier = null;

	public DocumentFactory configure(final Options<String> options) {
		final String algorithm = options.get("id-digest-method").value().orElse("SHA-256");
		final Charset encoding = options.get("charset").parse().asCharset().orElse(StandardCharsets.UTF_8);
		final Optional<String> method = options.get("id-method").value();

		if (method.isPresent()) {
			switch (method.get()) {
				case "path":
					this.identifier = new PathIdentifier();
					break;
				case "path-digest":
					this.identifier = new PathDigestIdentifier(algorithm, encoding);
					break;
				case "tika-digest":
					this.identifier = new TikaDigestIdentifier(algorithm);
					break;
				default:
					throw new IllegalArgumentException(String.format("\"%s\" is not a valid identifier.", method.get()));
			}
		} else {
			identifier = new TikaDigestIdentifier(algorithm);
		}

		return this;
	}

	public DocumentFactory withIdentifier(final Identifier identifier) {
		if (null == identifier) {
			throw new IllegalArgumentException("Identifier generator must not be null.");
		}

		this.identifier = identifier;
		return this;
	}

	public Document create(final String id, final Path path) {
		return new Document(id, identifier, path);
	}

	public Document create(final String id, final Path path, final Metadata metadata) {
		return new Document(id, identifier, path, metadata);
	}

	public Document create(final Path path) {
		return new Document(identifier, path);
	}

	public Document create(final Path path, final Metadata metadata) {
		return new Document(identifier, path, metadata);
	}

	public Document create(final URL url) throws URISyntaxException {
		return create(Paths.get(url.toURI()));
	}
}
