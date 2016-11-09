package org.icij.extract.parser.ocr;

import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ocr.TesseractOCRParser;

import java.util.HashSet;
import java.util.Set;

public class ExtendedTesseractOCRParser extends TesseractOCRParser {

	private static final long serialVersionUID = -2625994530917375952L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		Set<MediaType> types = super.getSupportedTypes(context);

		if (types.isEmpty()) {
			return types;
		}

		types = new HashSet<>();

		types.add(MediaType.image("jpx"));
		types.add(MediaType.image("jp2"));
		types.add(MediaType.image("x-portable-pixmap"));

		return types;
	}
}
