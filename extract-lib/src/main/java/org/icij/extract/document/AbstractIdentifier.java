package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class AbstractIdentifier implements Identifier {

	private final String key;

	final String algorithm;
	final Charset charset;

	public AbstractIdentifier(final String algorithm, final Charset charset) {
		key = Identifier.getKey(algorithm);
		this.algorithm = algorithm;
		this.charset = charset;
	}

	public AbstractIdentifier() {
		this("SHA256", StandardCharsets.US_ASCII);
	}

	@Override
	public String hash(final TikaDocument tikaDocument) {
		return retrieveHash(tikaDocument.getMetadata());
	}

	@Override
	public String retrieveHash(final Metadata metadata) {
		return metadata.get(key);
	}
}
