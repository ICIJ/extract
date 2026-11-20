package org.icij.extract.parser;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.parser.QRCodeParserDecorator.QR_CODES;
import static org.icij.extract.qrcode.QRCodeImages.code;
import static org.icij.extract.qrcode.QRCodeImages.write;
import static org.junit.Assert.fail;

import com.google.zxing.BarcodeFormat;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.image.ImageParser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.parser.RenderedPagesQRCodeDecorator.Target;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.ContentHandler;

public class RenderedPagesQRCodeDecoratorTest {
    private static final String ONE = "https://example.org/qr/one";

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test_restores_previous_target_after_failure() throws Exception {
        Path png = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        Metadata pdfMetadata = new Metadata();
        Target other = new Target(new Metadata());
        ParseContext context = new ParseContext();
        context.set(Target.class, other);
        RenderedPagesQRCodeDecorator decorator = new RenderedPagesQRCodeDecorator(new FailingPageParser());

        try (InputStream stream = Files.newInputStream(png)) {
            decorator.parse(stream, new BodyContentHandler(), pdfMetadata, context);
            fail("expected the stub parser to fail");
        } catch (TikaException expected) {
            assertThat(context.get(Target.class)).isSameAs(other);
        }

        assertThat(List.of(pdfMetadata.getValues(QR_CODES))).isEqualTo(List.of(ONE));
    }

    private static class FailingPageParser extends AbstractParser {
        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Set.of(MediaType.image("png"));
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
                throws TikaException {
            Metadata page = new Metadata();
            page.set(Metadata.CONTENT_TYPE, "image/png");
            page.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE, TikaCoreProperties.EmbeddedResourceType.RENDERING.name());
            try {
                new QRCodeParserDecorator(new ImageParser()).parse(stream, handler, page, context);
            } catch (Exception e) {
                throw new TikaException("page parse failed", e);
            }
            throw new TikaException("page failed after decoding");
        }
    }
}
