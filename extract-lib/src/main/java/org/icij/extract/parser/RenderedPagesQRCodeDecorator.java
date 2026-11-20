package org.icij.extract.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class RenderedPagesQRCodeDecorator extends ParserDecorator {
    @Serial
    private static final long serialVersionUID = 6127436085432671342L;

    public record Target(Metadata metadata) {}

    public RenderedPagesQRCodeDecorator(Parser parser) {
        super(parser);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        Target previous = context.get(Target.class);
        context.set(Target.class, new Target(metadata));
        try {
            super.parse(stream, handler, metadata, context);
        } finally {
            context.set(Target.class, previous);
        }
    }
}
