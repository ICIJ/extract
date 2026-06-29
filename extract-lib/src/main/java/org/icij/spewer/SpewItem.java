package org.icij.spewer;

import org.icij.extract.document.TikaDocument;

/**
 * One embedded document that is ready to be written to the spewer, carrying the same arguments the
 * legacy tree walk passes to {@link Spewer#writeDocument(TikaDocument, TikaDocument, TikaDocument, int)}:
 * the embed, its immediate parent, the top-level root, and its 1-based nesting level.
 */
public record SpewItem(TikaDocument embed, TikaDocument parent, TikaDocument root, int level) {}
