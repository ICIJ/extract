package org.icij.extract.extractor;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;

import static java.util.Optional.ofNullable;

public class EmbeddedDocumentMemoryExtractor {
    private final Parser parser;
    private final DigestingParser.Digester digester;
    private final String algorithm;

    public EmbeddedDocumentMemoryExtractor(final UpdatableDigester digester) { this(digester, digester.algorithm, false);}

    public EmbeddedDocumentMemoryExtractor(final DigestingParser.Digester digester, String algorithm, boolean ocr) {
        this.parser = new DigestingParser(ocr?new AutoDetectParser():createParserWithoutOCR(), digester);
        this.digester = digester;
        this.algorithm = algorithm;
    }

    public TikaDocumentSource extract(final TikaDocument rootDocument, final String embeddedDocumentDigest) throws SAXException, TikaException, IOException {
        ParseContext context = new ParseContext();
        ContentHandler handler = new BodyContentHandler(-1);
        context.set(Parser.class, parser);

        DigestEmbeddedDocumentExtractor extractor = new DigestEmbeddedDocumentExtractor(rootDocument, embeddedDocumentDigest, context, digester, algorithm);
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, extractor);

        parser.parse(new FileInputStream(rootDocument.getPath().toFile()), handler, rootDocument.getMetadata(), context);

        return extractor.getDocument();
    }

    static class DigestEmbeddedDocumentExtractor extends EmbedParser {
        private final String digestToFind;
        private final DigestingParser.Digester digester;
        private final String algorithm;
        private TikaDocumentSource document = null;
        private final LinkedList<TikaDocument> documentStack = new LinkedList<>();

        private DigestEmbeddedDocumentExtractor(final TikaDocument rootDocument, final String digest, ParseContext context, DigestingParser.Digester digester, String algorithm) {
            super(rootDocument, context);
            this.digestToFind = digest;
            this.digester = digester;
            this.algorithm = algorithm;
            this.documentStack.add(rootDocument);
        }

        @Override
        public void delegateParsing(InputStream stream, ContentHandler handler, Metadata metadata) throws IOException, SAXException {
            if (document != null) return;
            EmbeddedTikaDocument embed = this.documentStack.getLast().addEmbed(metadata);

            try (final TikaInputStream tis = TikaInputStream.get(CloseShieldInputStream.wrap(stream))) {
                if (stream instanceof TikaInputStream) {
                    final Object container = ((TikaInputStream) stream).getOpenContainer();

                    if (container != null) {
                        tis.setOpenContainer(container);
                    }
                }
                digester.digest(tis, metadata, context);
                if (stream.getClass().cast(stream).markSupported()) { // Avoid mark/reset error for unsupported inputstreams
                    tis.reset();
                }
                String digest;
                try {
                    digest = new DigestIdentifier(algorithm, Charset.defaultCharset()).generateForEmbed(embed);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
                if (digestToFind.equals(digest)) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int nbTmpBytesRead;
                    for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = tis.read(tmp)) > 0; ) {
                        buffer.write(tmp, 0, nbTmpBytesRead);
                    }
                    this.document = new TikaDocumentSource(metadata, buffer.toByteArray());
                } else {
                    this.documentStack.add(embed);
                    super.delegateParsing(tis, handler, metadata);
                }
            } finally {
                this.documentStack.removeLast();
            }
        }

        public TikaDocumentSource getDocument() {
            return ofNullable(document).orElseThrow(() ->
                    new ContentNotFoundException(documentStack.get(0).getPath().toString(), digestToFind)
            );
        }
    }

    private Parser createParserWithoutOCR() {
        try {
            return new AutoDetectParser(new TikaConfig(new ByteArrayInputStream(("" +
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<properties><parsers><parser class=\"org.apache.tika.parser.DefaultParser\">" +
                    "<parser-exclude class=\"org.apache.tika.parser.ocr.TesseractOCRParser\"/>" +
                    "</parser></parsers></properties>").getBytes())));
        } catch (TikaException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ContentNotFoundException extends NullPointerException {
        ContentNotFoundException(String rootId, String embedId) {
            super("<" + embedId + "> embedded document not found in root document " + rootId);
        }
    }
}
