package org.icij.extract.document;

public class PathIdentifier extends AbstractIdentifier {

	@Override
	public String generate(final TikaDocument tikaDocument) {
		return tikaDocument.getPath().toString();
	}

	@Override
	public String generateForEmbed(final EmbeddedTikaDocument embed) {
		return generate(embed);
	}
}
