/*
 * An HTML5-only version of Validator.nu's HTML serializer. The original license
 * is reproduced below.
 *
 * Copyright (c) 2003, 2004 Henri Sivonen and Taavi Hupponen
 * Copyright (c) 2006 Henri Sivonen
 *
 * Permission is hereby granted, free of charge, to any person obtaining a 
 * copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation 
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
 * DEALINGS IN THE SOFTWARE.
 */

package org.icij.extract.sax;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Serializes a sequence of SAX events representing an XHTML 1.0 Strict document
 * to a <code>Writer</code> as a UTF-8-encoded HTML 5 document. The SAX events 
 * must represent a valid XHTML 1.0 document, except the namespace prefixes
 * don't matter and there may be <code>startElement</code> and <code>endElement</code>
 * calls for elements from other namespaces. The <code>startElement</code> and
 * <code>endElement</code> calls for non-XHTML elements are ignored. No
 * validity checking is performed. Hence, the emitter of the SAX events is
 * responsible for making sure the events represent a document that meets the
 * above requirements. The <code>Writer</code> is not closed when the end of
 * the document is seen.
 *
 * @since 1.0.0-beta
 */
public class HTML5Serializer implements ContentHandler {

	/**
	 * The XHTML namespace URI
	 */
	private final static String XHTML_NS = "http://www.w3.org/1999/xhtml";

	/**
	 * HTML 4.01 elements which don't have an end tag
	 */
	private static final String[] emptyElements = { "area", "base", "basefont",
			"br", "col", "command", "frame", "hr", "img", "input", "isindex",
			"link", "meta", "param" };

	/**
	 * Minimized "boolean" HTML attributes
	 */
	private static final String[] booleanAttributes = { "active", "async",
			"autofocus", "autosubmit", "checked", "compact", "declare",
			"default", "defer", "disabled", "ismap", "multiple", "nohref",
			"noresize", "noshade", "nowrap", "readonly", "required", "selected" };

	/**
	 * The writer used for output
	 */
	private final Writer writer;

	/**
	 * Creates a new instance of HtmlSerializer in the HTML 4.01 doctype mode
	 * with the UTF-8 encoding and no charset meta.
	 * 
	 * @param writer the writer to which the output is written
	 */
	public HTML5Serializer(final Writer writer) {
		this.writer = writer;
	}

	/**
	 * Writes out characters.
	 * 
	 * @param ch the source array
	 * @param start the index of the first character to be written
	 * @param length the number of characters to write
	 * @throws SAXException if there are IO problems
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		try {
			for (int j = 0; j < length; j++) {
				char c = ch[start + j];
				switch (c) {
					case '<':
						writer.write("&lt;");
						break;
					case '>':
						writer.write("&gt;");
						break;
					case '&':
						writer.write("&amp;");
						break;
					default:
						writer.write(c);
				}
			}
		} catch (IOException e) {
			throw (SAXException) new SAXException(e).initCause(e);
		}
	}

	/**
	 * Must be called last.
	 *
	 * @throws SAXException if there are IO problems
	 */
	@Override
	public void endDocument() throws SAXException {
		try {
			writer.write('\n');
		} catch (IOException e) {
			throw (SAXException) new SAXException(e).initCause(e);
		}
	}

	/**
	 * Writes an end tag if the element is an XHTML element and is not an empty
	 * element in HTML 4.01 Strict.
	 * 
	 * @param namespaceURI the XML namespace
	 * @param localName the element name in the namespace
	 * @param qName ignored
	 * @throws SAXException if there are IO problems
	 */
	@Override
	public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
		try {
			if (XHTML_NS.equals(namespaceURI) && Arrays.binarySearch(emptyElements, localName) < 0) {
				writer.write("</");
				writer.write(localName);
				writer.write('>');
			}
		} catch (IOException e) {
			throw (SAXException) new SAXException(e).initCause(e);
		}
	}

	/**
	 * Must be called first.
	 */
	@Override
	public void startDocument() throws SAXException {
		try {
			writer.write("<!DOCTYPE html>\n");
		} catch (IOException e) {
			throw (SAXException) new SAXException(e).initCause(e);
		}
	}

	/**
	 * Writes a start tag if the element is an XHTML element.
	 * 
	 * @param namespaceURI the XML namespace
	 * @param localName the element name in the namespace
	 * @param qName ignored
	 * @param atts the attribute list
	 * @throws SAXException if there are IO problems
	 */
	@Override
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
		try {
			if (XHTML_NS.equals(namespaceURI)) {

				if ("meta".equals(localName)
						&& ((atts.getIndex("", "http-equiv") != -1) || (atts.getIndex(
								"", "httpequiv") != -1))) {
					return;
				}

				// start and element name
				writer.write('<');
				writer.write(localName);

				// attributes
				int length = atts.getLength();
				boolean langPrinted = false;
				for (int i = 0; i < length; i++) {
					String ns = atts.getURI(i);
					String name = null;
					if ("".equals(ns)) {
						name = atts.getLocalName(i);
					} else if ("http://www.w3.org/XML/1998/namespace".equals(ns)
							&& "lang".equals(atts.getLocalName(i))) {
						name = "lang";
					}
					if (name != null && !(langPrinted && "lang".equals(name))) {
						writer.write(' ');
						writer.write(name);
						if ("lang".equals(name)) {
							langPrinted = true;
						}
						if (Arrays.binarySearch(booleanAttributes, name) < 0) {
							// write value, escape certain characters
							writer.write("=\"");
							String value = atts.getValue(i);
							for (int j = 0; j < value.length(); j++) {
								char c = value.charAt(j);
								switch (c) {
									case '<':
										writer.write("&lt;");
										break;
									case '>':
										writer.write("&gt;");
										break;
									case '&':
										writer.write("&amp;");
										break;
									case '"':
										writer.write("&quot;");
										break;
									default:
										writer.write(c);
								}
							}

							writer.write('"');
						}
					}
				}

				// close
				writer.write('>');
				if ("head".equals(localName)) {
					writer.write("<meta charset=\"UTF-8\">");
				}
			}
		} catch (IOException e) {
			throw (SAXException) new SAXException(e).initCause(e);
		}
	}

	/**
	 * This method does nothing.
	 */
	@Override
	public void endPrefixMapping(String str) throws SAXException {
	}

	/**
	 * This method does nothing.
	 */
	@Override
	public void ignorableWhitespace(char[] values, int param, int param2) throws SAXException {
	}

	/**
	 * This method does nothing.
	 */
	@Override
	public void processingInstruction(String str, String str1) throws SAXException {
	}

	/**
	 * This method does nothing.
	 */
	@Override
	public void setDocumentLocator(Locator locator) {
	}

	/**
	 * This method does nothing.
	 */
	@Override
	public void skippedEntity(String str) throws SAXException {
	}

	/**
	 * This method does nothing.
	 */
	@Override
	public void startPrefixMapping(String str, String str1) throws SAXException {
	}
}
