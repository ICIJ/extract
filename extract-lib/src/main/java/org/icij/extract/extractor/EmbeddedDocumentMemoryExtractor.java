package org.icij.extract.extractor;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.Identifier;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class EmbeddedDocumentMemoryExtractor {
    private final Parser parser;
    private final String digesterModifier;
    private final String digesterAlgorithm;
    private TikaConfig config = TikaConfig.getDefaultConfig();

    EmbeddedDocumentMemoryExtractor(final String digesterModifier, final String digesterAlgorithm) {
        this.digesterModifier = digesterModifier;
        this.digesterAlgorithm = digesterAlgorithm;
        parser = new DigestingParser(new AutoDetectParser(), new UpdatableDigester(digesterModifier, digesterAlgorithm));
    }

    public byte[] extract(final InputStream stream, final String documentDigest) throws SAXException, TikaException, IOException {
        Metadata metadata = new Metadata();
        ParseContext context = new ParseContext();
        ContentHandler handler = new BodyContentHandler(-1);
        context.set(Parser.class, parser);

        DigestEmbeddedDocumentExtractor extractor = new DigestEmbeddedDocumentExtractor(documentDigest, context,
                new UpdatableDigester(digesterModifier, digesterAlgorithm));
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, extractor);

        parser.parse(stream, handler, metadata, context);

        return extractor.getBytes();
    }

    static class DigestEmbeddedDocumentExtractor extends ParsingEmbeddedDocumentExtractor {
        private final String digest;
        private final ParseContext context;
        private final UpdatableDigester digester;
        private byte[] fileContent = null;

        private DigestEmbeddedDocumentExtractor(final String digest, ParseContext context,UpdatableDigester digester) {
            super(context);
            this.digest = digest;
            this.context = context;
            this.digester = digester;
        }

        @Override
        public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) throws IOException {
            if (fileContent != null) return;

            digester.digest(stream, metadata, context);
            if (digest.equals(metadata.get(Identifier.getKey(digester.algorithm)))) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                long nbBytesRead = 0L;
                int nbTmpBytesRead;
                for(byte[] tmp = new byte[8192]; (nbTmpBytesRead = stream.read(tmp)) > 0; nbBytesRead += (long)nbTmpBytesRead) {
                    buffer.write(tmp, 0, nbTmpBytesRead);
                }
                this.fileContent = buffer.toByteArray();
            }
        }

        public byte[] getBytes() {
            return fileContent;
        }
    }
}
