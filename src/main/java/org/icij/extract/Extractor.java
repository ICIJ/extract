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
import org.apache.tika.parser.DelegatingParser;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;

import org.apache.tika.mime.MediaType;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.CloseShieldInputStream;

import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;

import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

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

	public ParsingReader extract(final Path file) throws IOException, FileNotFoundException, TikaException {
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

		// By default, only the object IDs are used for determining uniqueness.
		// In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
		pdfConfig.setExtractUniqueInlineImagesOnly(false);
		pdfConfig.setUseNonSequentialParser(true);
		parseContext.set(PDFParserConfig.class, pdfConfig);

		final TikaInputStream inputStream = TikaInputStream.get(new FileInputStream(file.toString()));

		// Set up recursive parsing of archives and documents with embedded images.
		parseContext.set(Parser.class, parser);
		parseContext.set(EmbeddedDocumentExtractor.class, new ParsingEmbeddedDocumentExtractor(parseContext) {

			// This override is made for logging purposes, as by default exceptions are swallowed by ParsingEmbeddedDocumentExtractor.
			// See extractImages here: http://svn.apache.org/viewvc/tika/trunk/tika-parsers/src/main/java/org/apache/tika/parser/pdf/PDF2XHTML.java?view=markup
			// And this: https://svn.apache.org/repos/asf/tika/trunk/tika-core/src/main/java/org/apache/tika/extractor/ParsingEmbeddedDocumentExtractor.java
			@Override
			public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) throws SAXException, IOException {
				logger.info("Extracting embedded document from document: " + file + ".");

				TemporaryResources tmp = new TemporaryResources();
				try {
					final TikaInputStream newStream = TikaInputStream.get(new CloseShieldInputStream(stream), tmp);

					if (stream instanceof TikaInputStream) {
						final Object container = ((TikaInputStream) stream).getOpenContainer();

						if (container != null) {
							newStream.setOpenContainer(container);
						}
					}

					new DelegatingParser().parse(newStream, new EmbeddedContentHandler(new BodyContentHandler(handler)), metadata, parseContext);
				} catch (EncryptedDocumentException e) {
					logger.log(Level.WARNING, "Encrypted document embedded in document: " + file + ".", e);
				} catch (TikaException e) {
					logger.log(Level.WARNING, "Unable to parse embedded document in document: " + file + ".", e);
				} finally {
					tmp.close();
				}
			}
		});

		final ParsingReader reader = new ParsingReader(parser, inputStream, metadata, parseContext);

		return reader;
	}
}
