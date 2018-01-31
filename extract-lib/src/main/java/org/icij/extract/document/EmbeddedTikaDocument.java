package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

import java.nio.file.Path;

public class EmbeddedTikaDocument extends TikaDocument {

	private final TikaDocument parent;

	EmbeddedTikaDocument(final TikaDocument parent, final Metadata metadata) {
		super(parent.getIdentifier(), parent.getPath(), metadata);
		this.parent = parent;
	}

	EmbeddedTikaDocument(final TikaDocument parent, final Identifier identifier, final Path path, final Metadata metadata) {
		super(identifier, path, metadata);
		this.parent = parent;
	}

	@Override
	String generateId() throws Exception {
		return getIdentifier().generateForEmbed(this);
	}

	TikaDocument getParent() {
		return parent;
	}
}
