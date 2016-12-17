package org.icij.extract.document;

public class PathIdentifier implements Identifier {

	@Override
	public String generate(final Document document) {
		return document.getPath().toString();
	}
}
