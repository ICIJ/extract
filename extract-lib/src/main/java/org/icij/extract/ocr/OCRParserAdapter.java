package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class OCRParserAdapter<P extends Parser> implements Parser {
    private static final Set<MediaType> JPEG2000_TYPES = Set.of(
        MediaType.image("jp2"),
        MediaType.image("jpx"),
        MediaType.image("jpm"),
        MediaType.image("jpeg2000"),
        MediaType.image("j2k"),
        MediaType.image("j2c"),
        MediaType.image("ocr-jp2"),
        MediaType.image("ocr-jpx")
    );

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
        // Workaround for JP2/JPX images
        String contentType = metadata.get(Metadata.CONTENT_TYPE);
        MediaType mediaType = contentType == null ? null : MediaType.parse(contentType);
        if (mediaType != null && JPEG2000_TYPES.contains(mediaType.getBaseType())) {
            stream = convertJp2ToPng(stream, metadata);
            try {
                delegatedParser.parse(stream, handler, metadata, parseContext);
            } finally {
                metadata.set(Metadata.CONTENT_TYPE, contentType);
            }
        } else {
            delegatedParser.parse(stream, handler, metadata, parseContext);
        }
    }

    private static InputStream convertJp2ToPng(InputStream stream, Metadata metadata) throws IOException {
        BufferedImage image = ImageIO.read(stream);
        if (image == null) {
            throw new IOException("Failed to decode JP2/JPX image");
        }
        // Leptonica rejects indexed/palettized PNGs — normalize to RGB
        if (image.getType() != BufferedImage.TYPE_INT_RGB) {
            BufferedImage rgb = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
            rgb.createGraphics().drawImage(image, 0, 0, null);
            image = rgb;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(image, "png", out);
        metadata.set(Metadata.CONTENT_TYPE, "image/png");
        return new ByteArrayInputStream(out.toByteArray());
    }
}
