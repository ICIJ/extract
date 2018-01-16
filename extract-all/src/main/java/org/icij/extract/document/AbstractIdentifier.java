package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class AbstractIdentifier implements Identifier {

	private final String key;

	final String algorithm;
	final Charset charset;

	public AbstractIdentifier(final String algorithm, final Charset charset) {
		key = TikaCoreProperties.TIKA_META_PREFIX + "digest" + Metadata.NAMESPACE_PREFIX_DELIMITER + algorithm
				.replace("-", "");
		this.algorithm = algorithm;
		this.charset = charset;
	}

	public AbstractIdentifier() {
		this("SHA256", StandardCharsets.US_ASCII);
	}

	@Override
	public String hash(final Document document) {
		return retrieveHash(document.getMetadata());
	}

	@Override
	public String retrieveHash(final Metadata metadata) {
		return metadata.get(key);
	}
}
