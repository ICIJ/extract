package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
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
		final String algorithm = options.get("idDigestMethod").value().orElse("SHA-256");
		final Charset charset = options.get("charset").parse().asCharset().orElse(StandardCharsets.UTF_8);
		final Optional<String> method = options.get("idMethod").value();

		if (method.isPresent()) {
			switch (method.get()) {
				case "path":
					this.identifier = new PathIdentifier();
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

	public Document create(final String id, final Path path) {
		return new Document(id, identifier, path);
	}

	public Document create(final String id, final Path path, final Metadata metadata) {
		return new Document(id, identifier, path, metadata);
	}

	public Document create(final Path path) {
		return new Document(identifier, path);
	}

	public Document create(final String path) {
		return create(Paths.get(path));
	}

	public Document create(final String id, final String path) {
		return create(id, Paths.get(path));
	}

	public Document create(final Path path, final Metadata metadata) {
		return new Document(identifier, path, metadata);
	}

	public Document create(final URL url) throws URISyntaxException {
		return create(Paths.get(url.toURI()));
	}
}
