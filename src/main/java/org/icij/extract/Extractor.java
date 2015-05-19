package org.icij.extract;

import java.util.Map;
import java.util.Set;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.file.Path;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParsingReader;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.mime.MediaType;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.exception.TikaException;

import org.xml.sax.ContentHandler;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Extractor {
	private final Logger logger;

	private Charset outputEncoding = StandardCharsets.UTF_8;
	private String ocrLanguage = "eng";

	private final Path file;

	private static final TikaConfig tikaConfig = TikaConfig.getDefaultConfig();

	public Extractor (Logger logger, Path file) {
		this.logger = logger;
		this.file = file;
	}

	public void setOutputEncoding(Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public Charset getOutputEncoding() {
		return outputEncoding;
	}

	public void setOcrLanguage(String ocrLanguage) {
		this.ocrLanguage = ocrLanguage;
	}

	public ParsingReader extract(Path file) throws IOException, FileNotFoundException, TikaException {
		final Metadata metadata = new Metadata();

		metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, file.getFileName().toString());

		final AutoDetectParser parser = new AutoDetectParser(tikaConfig);
		final Map<MediaType, Parser> parsers = parser.getParsers();

		parsers.put(MediaType.APPLICATION_XML, new HtmlParser());
		parser.setParsers(parsers);

		parser.setFallback(new Parser() {
			public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
				return parser.getSupportedTypes(parseContext);
			}

			public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata, ParseContext parseContext) throws TikaException {
				throw new TikaException("Unsupported media type: " + metadata.get(HttpHeaders.CONTENT_TYPE) + ".");
			}
		});

		final ParseContext parseContext = new ParseContext();

		final TesseractOCRConfig ocrConfig = new TesseractOCRConfig();

		ocrConfig.setLanguage(ocrLanguage);
		parseContext.set(TesseractOCRConfig.class, ocrConfig);

		final PDFParserConfig pdfConfig = new PDFParserConfig();

		// Run OCR on images contained within PDFs.
		pdfConfig.setExtractInlineImages(true);
		pdfConfig.setExtractUniqueInlineImagesOnly(true);
		parseContext.set(PDFParserConfig.class, pdfConfig);

		final TikaInputStream inputStream = TikaInputStream.get(new FileInputStream(file.toString()));

		// Set up recursive parsing of archives.
		// See: http://wiki.apache.org/tika/RecursiveMetadata
		parseContext.set(Parser.class, parser);

		final ParsingReader read = new ParsingReader(parser, inputStream, metadata, parseContext);

		return read;
	}
}
