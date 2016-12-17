package org.icij.extract.document;

@FunctionalInterface
public interface Identifier {

	String generate(final Document document);
}
