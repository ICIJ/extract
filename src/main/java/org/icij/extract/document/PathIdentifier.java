package org.icij.extract.document;

public class PathIdentifier extends AbstractIdentifier {

	@Override
	public String generate(final Document document) {
		return document.getPath().toString();
	}

	@Override
	public String generateForEmbed(final EmbeddedDocument embed) {
		return generate(embed);
	}
}
