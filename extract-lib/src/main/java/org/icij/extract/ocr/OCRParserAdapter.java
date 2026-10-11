package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.apache.tika.metadata.TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE;

public class OCRParserAdapter<P extends Parser> implements Parser {
    private static final Logger LOGGER = LoggerFactory.getLogger(OCRParserAdapter.class);
    private static final String TESSERACT_OUTPUTS = "apache-tika-*.tmp.{txt,osd,hocr}";
    // Tika's AbstractImageParser routes images through OCR by rewriting their media type to a
    // synthetic "image/ocr-<fmt>" form (see AbstractImageParser#convertToOCRMediaType /
    // OCR_MEDIATYPE_PREFIX) and stashing it in CONTENT_TYPE_PARSER_OVERRIDE, which Tika later promotes
    // onto Content-Type. Left untouched, that internal routing type is what gets persisted as the
    // document's content type (e.g. "image/ocr-jpeg" instead of "image/jpeg"). We strip the prefix
    // once OCR is done so consumers see the real media type.
    private static final String OCR_SUBTYPE_PREFIX = "ocr-";

    private final P delegatedParser;

    public OCRParserAdapter(P delegatedParser) {
        this.delegatedParser = delegatedParser;
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return this.delegatedParser.getSupportedTypes(parseContext);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext parseContext) throws IOException, SAXException, TikaException {
        if(delegatedParser == null){
            throw new NullPointerException("Parser is null");
        }
        metadata.set(OCRParser.OCR_PARSER, delegatedParser.getClass().getName());
        try {
            delegatedParser.parse(stream, handler, metadata, parseContext);
        } catch (IOException | SAXException | TikaException | RuntimeException e) {
            if (delegatedParser instanceof TesseractOCRParser) {
                deleteOrphanTesseractOutputs();
            }
            throw e;
        } finally {
            // The delegated OCR parser has already resolved its input format from
            // CONTENT_TYPE_PARSER_OVERRIDE, so it is now safe to rewrite the routing type back to the real
            // media type. Stripping the override is the effective fix (Tika promotes it onto Content-Type
            // after this parse returns); Content-Type is normalised too as a defensive measure.
            // This runs in a finally because a failed OCR parse (Tess4JOCRParser rethrows IOException /
            // TikaException, e.g. on the OCR timeout) still leaves the embed indexed by EmbedSpawner, so
            // the routing type would otherwise become the document's persisted content type.
            restoreMediaTypes(metadata);
        }
    }

    // When tesseract fails, Tika deletes its apache-tika-*.tmp base file but not the output next to it.
    private static void deleteOrphanTesseractOutputs() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        try (DirectoryStream<Path> outputs = Files.newDirectoryStream(tmp, TESSERACT_OUTPUTS)) {
            outputs.forEach(OCRParserAdapter::deleteIfOrphan);
        } catch (IOException | DirectoryIteratorException e) {
            LOGGER.debug("could not list tesseract outputs: {}", e.toString());
        }
    }

    private static void deleteIfOrphan(Path output) {
        String name = output.getFileName().toString();
        Path base = output.resolveSibling(name.substring(0, name.lastIndexOf('.')));
        try {
            if (Files.notExists(base)) {
                Files.deleteIfExists(output);
            }
        } catch (IOException e) {
            LOGGER.debug("could not delete tesseract output {}: {}", output, e.toString());
        }
    }

    static void restoreMediaTypes(final Metadata metadata) {
        restoreMediaType(metadata, CONTENT_TYPE_PARSER_OVERRIDE);
        restoreMediaType(metadata, Metadata.CONTENT_TYPE);
    }

    private static void restoreMediaType(final Metadata metadata, final Property field) {
        final String restored = stripOcrPrefix(metadata.get(field));
        if (restored != null) {
            metadata.set(field, restored);
        }
    }

    private static void restoreMediaType(final Metadata metadata, final String field) {
        final String restored = stripOcrPrefix(metadata.get(field));
        if (restored != null) {
            metadata.set(field, restored);
        }
    }

    // Returns the de-OCR-ed media type when contentType is an "image/ocr-*" routing type, or null when
    // there is nothing to change (value absent, unparseable, or not an OCR routing type).
    static String stripOcrPrefix(final String contentType) {
        if (contentType == null) {
            return null;
        }
        final MediaType mediaType = MediaType.parse(contentType);
        if (mediaType == null) {
            return null;
        }
        final String subtype = mediaType.getSubtype();
        if (!subtype.startsWith(OCR_SUBTYPE_PREFIX)) {
            return null;
        }
        return new MediaType(mediaType.getType(), subtype.substring(OCR_SUBTYPE_PREFIX.length())).toString();
    }
}
