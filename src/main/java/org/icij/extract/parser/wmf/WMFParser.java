package org.icij.extract.parser.wmf;

import org.apache.batik.transcoder.wmf.WMFConstants;
import org.apache.batik.transcoder.wmf.tosvg.GdiObject;
import org.apache.batik.transcoder.wmf.tosvg.MetaRecord;
import org.apache.batik.transcoder.wmf.tosvg.WMFFont;
import org.apache.batik.transcoder.wmf.tosvg.WMFPainter;
import org.apache.batik.transcoder.wmf.tosvg.WMFRecordStore;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Set;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

public class WMFParser extends AbstractParser {

	private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MediaType.application("x-msmetafile"));
	private static final String WMF_MIME_TYPE = "application/x-msmetafile";
	private static final long serialVersionUID = 5516989102471431040L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata, final
	ParseContext context) throws IOException, SAXException, TikaException {
		metadata.set(Metadata.CONTENT_TYPE, WMF_MIME_TYPE);

		final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
		xhtml.startDocument();

		final WMFRecordStore store = new WMFRecordStore();
		WMFFont wmfFont = null;

		store.read(new DataInputStream(stream));

		metadata.set("height", Integer.toString(store.getHeightPixels()));
		metadata.set("width", Integer.toString(store.getWidthPixels()));

		final int numRecords = store.getNumRecords();
		final int numObjects = store.getNumObjects();

		for (int i = 0; i < numRecords; i++) {
			final MetaRecord record = store.getRecord(i);

			// Logic borrowed from Batik's WMFTranscoder and WMFPainter.
			switch (record.functionId) {
				case WMFConstants.META_SELECTOBJECT:
					int gdiIndex = record.ElementAt( 0 );

					if ((gdiIndex & 0x80000000 ) != 0) {
						break;
					}

					if (gdiIndex >= numObjects) {
						gdiIndex -= numObjects;
					}

					GdiObject gdiObj = store.getObject(gdiIndex);

					if (!gdiObj.isUsed()) {
						break;
					}

					if (gdiObj.getType() == WMFPainter.FONT) {
						wmfFont = (WMFFont) gdiObj.getObject();
					}

					break;

				case WMFConstants.META_TEXTOUT:
				case WMFConstants.META_DRAWTEXT:
				case WMFConstants.META_EXTTEXTOUT:
					final String str = decodeString(((MetaRecord.ByteRecord) record).bstr, wmfFont);
					final char[] chr = str.toCharArray();

					xhtml.startElement(XHTML, "p", "p", new AttributesImpl());
					xhtml.characters(chr, 0, chr.length);
					xhtml.endElement(XHTML, "p", "p");

					break;
			}
		}

		xhtml.endDocument();
	}

	private String decodeString(final byte[] buffer, final WMFFont wmfFont) {
		final String charset;

		if (null == wmfFont) {
			charset = WMFConstants.CHARSET_ANSI;
		} else {
			switch (wmfFont.charset) {
				case WMFConstants.META_CHARSET_DEFAULT:
					charset = WMFConstants.CHARSET_DEFAULT;
					break;
				case WMFConstants.META_CHARSET_GREEK:
					charset = WMFConstants.CHARSET_GREEK;
					break;
				case WMFConstants.META_CHARSET_RUSSIAN:
					charset = WMFConstants.CHARSET_CYRILLIC;
					break;
				case WMFConstants.META_CHARSET_HEBREW:
					charset = WMFConstants.CHARSET_HEBREW;
					break;
				case WMFConstants.META_CHARSET_ARABIC:
					charset = WMFConstants.CHARSET_ARABIC;
					break;
				case WMFConstants.META_CHARSET_ANSI:
				default:
					charset = WMFConstants.CHARSET_ANSI;
					break;
			}
		}

		try {
			return new String(buffer, charset);
		} catch (UnsupportedEncodingException e) {
			return new String(buffer);
		}
	}
}
