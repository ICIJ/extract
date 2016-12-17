package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

import java.nio.file.Path;

public class EmbeddedDocument extends Document {

	private final Document parent;

	EmbeddedDocument(final Document parent, final Metadata metadata) {
		super(parent.getIdentifier(), parent.getPath(), metadata);
		this.parent = parent;
	}

	EmbeddedDocument(final Document parent, final Identifier identifier, final Path path, final Metadata metadata) {
		super(identifier, path, metadata);
		this.parent = parent;
	}

	Document getParent() {
		return parent;
	}
}
