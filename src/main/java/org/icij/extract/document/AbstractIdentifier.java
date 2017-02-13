package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class AbstractIdentifier implements Identifier {

	private final String key;
	final String algorithm;
	final Charset charset;

	AbstractIdentifier(final String algorithm, final Charset charset) {
		key = TikaCoreProperties.TIKA_META_PREFIX + "digest" + Metadata.NAMESPACE_PREFIX_DELIMITER + algorithm
				.replace("-", "");
		this.algorithm = algorithm;
		this.charset = charset;
	}

	AbstractIdentifier() {
		this("SHA256", StandardCharsets.US_ASCII);
	}

	@Override
	public String hash(final Document document) {
		final String hash = document.getMetadata().get(key);

		if (null == hash) {
			throw new RuntimeException("Unexpected null hash. Check that the correct algorithm is specified.");
		}

		return hash;
	}
}
