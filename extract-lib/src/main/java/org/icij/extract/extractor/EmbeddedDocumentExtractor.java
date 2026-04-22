package org.icij.extract.extractor;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.DefaultEmbeddedStreamTranslator;
import org.apache.tika.extractor.EmbeddedStreamTranslator;
import org.apache.tika.io.TemporaryResources;
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
import org.icij.spewer.MetadataTransformer;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.lang.module.ModuleDescriptor;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

public class EmbeddedDocumentExtractor {
    private final Parser parser;
    private final DigestingParser.Digester digester;
    private final String algorithm;
    private final Path artifactPath;
    // for tests
    private final Function<TikaDocument, ModuleDescriptor.Version> tikaVersionSupplier;

    public EmbeddedDocumentExtractor(UpdatableDigester digester) {
        this(digester, false);
    }

    public EmbeddedDocumentExtractor(UpdatableDigester digester, boolean ocr) {
        this(digester, digester.algorithm, null, ocr);
    }

    public EmbeddedDocumentExtractor(final UpdatableDigester digester, Path artifactPath) {
        this(digester, digester.algorithm, artifactPath, false);
    }

    public EmbeddedDocumentExtractor(final DigestingParser.Digester digester, String algorithm, Path artifactPath, boolean ocr) {
        this(digester, algorithm, artifactPath, ocr, TikaDocument::getTikaVersion);
    }

    EmbeddedDocumentExtractor(final DigestingParser.Digester digester, String algorithm, Path artifactPath, boolean ocr, Function<TikaDocument, ModuleDescriptor.Version> tikaVersionSupplier) {
        this.parser = new DigestingParser(ocr ? new AutoDetectParser() : createParserWithoutOCR(), digester, false);
        this.digester = digester;
        this.artifactPath = artifactPath;
        this.algorithm = algorithm;
        this.tikaVersionSupplier = tikaVersionSupplier;
    }

    public void extractAll(final TikaDocument document) throws SAXException, TikaException, IOException {
        ofNullable(artifactPath).orElseThrow(() -> new IllegalStateException("cannot extract all embedded files in memory"));
        ParseContext context = new ParseContext();
        ContentHandler handler = new BodyContentHandler(-1);
        context.set(Parser.class, parser);

        DigestEmbeddedDocumentExtractor extractor = new DigestAllEmbeddedDocumentExtractor(document, context, digester, algorithm, artifactPath, tikaVersionSupplier);
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, extractor);

        parser.parse(new FileInputStream(document.getPath().toFile()), handler, document.getMetadata(), context);
    }

    public TikaDocumentSource extract(final TikaDocument rootDocument, final String embeddedDocumentDigest) throws SAXException, TikaException, IOException {
        if (artifactPath != null) {
            File cachedFile = getEmbeddedPath(artifactPath, embeddedDocumentDigest).toFile();
            if (cachedFile.exists()) {
                return new TikaDocumentSource(MetadataTransformer.loadMetadata(new File(cachedFile + ".json")), DigestEmbeddedDocumentFileExtractor.getFileInputStream(cachedFile));
            }
        }
        ParseContext context = new ParseContext();
        ContentHandler handler = new BodyContentHandler(-1);
        context.set(Parser.class, parser);

        DigestEmbeddedDocumentFileExtractor extractor = getExtractor(rootDocument, embeddedDocumentDigest, context, artifactPath);
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, extractor);

        parser.parse(new FileInputStream(rootDocument.getPath().toFile()), handler, rootDocument.getMetadata(), context);

        return extractor.getDocument();
    }

    private DigestEmbeddedDocumentFileExtractor getExtractor(TikaDocument rootDocument, String embeddedDocumentDigest, ParseContext context, Path artifactPath) {
        if (artifactPath != null) {
            return new DigestEmbeddedDocumentFileExtractor(rootDocument, embeddedDocumentDigest, context, digester, algorithm, artifactPath, tikaVersionSupplier);
        } else {
            return new DigestEmbeddedDocumentMemoryExtractor(rootDocument, embeddedDocumentDigest, context, digester, algorithm, tikaVersionSupplier);
        }
    }

    static Path getEmbeddedPath(Path artifactPath, String digest) {
        return ArtifactUtils.getEmbeddedPath(artifactPath, digest).resolve("raw");
    }

    private static abstract class DigestEmbeddedDocumentExtractor extends EmbedParser {
        private final DigestingParser.Digester digester;
        private final String algorithm;
        protected final Path artifactPath;
        protected final LinkedList<TikaDocument> documentStack = new LinkedList<>();
        private final EmbeddedStreamTranslator embeddedStreamTranslator = new DefaultEmbeddedStreamTranslator();
        private final Function<TikaDocument, ModuleDescriptor.Version> tikaVersionSupplier;


        DigestEmbeddedDocumentExtractor(TikaDocument document, ParseContext context, DigestingParser.Digester digester,
                                        String algorithm, Path artifactPath, Function<TikaDocument, ModuleDescriptor.Version> tikaVersionSupplier) {
            super(document, context);
            this.digester = digester;
            this.algorithm = algorithm;
            this.artifactPath = artifactPath;
            this.documentStack.add(document);
            this.tikaVersionSupplier = tikaVersionSupplier;
        }

        protected abstract boolean documentCallback(Metadata metadata, String digest, TikaInputStream tis) throws IOException;

        @Override
        void delegateParsing(InputStream stream, ContentHandler handler, Metadata metadata) throws IOException, SAXException {
            EmbeddedTikaDocument embed = this.documentStack.getLast().addEmbed(metadata);
            try (final TikaInputStream tis = TikaInputStream.get(CloseShieldInputStream.wrap(stream))) {
                if (stream instanceof TikaInputStream) {
                    final Object container = ((TikaInputStream) stream).getOpenContainer();

                    if (container != null) {
                        tis.setOpenContainer(container);
                    }
                }
                tis.mark(0); // Marking the position before resetting
                // this if/else is coming from DigestingParser since 3.3.0 to fix issues with Microsoft OLE docs
                // see https://issues.apache.org/jira/browse/TIKA-4533
                // only if version is > to 3.2.3
                if (embeddedStreamTranslator.shouldTranslate(tis, metadata) &&
                        tikaVersionSupplier.apply(documentStack.get(0)).compareTo(ModuleDescriptor.Version.parse("3.2.3")) > 0) {
                    Path translatedBytes;
                    try (TemporaryResources tmp = new TemporaryResources()) {
                        translatedBytes = tmp.createTempFile();
                        if (tis.getOpenContainer() == null) {
                            try (InputStream is = TikaInputStream.get(tis.getPath())) {
                                Files.copy(embeddedStreamTranslator.translate(is, metadata), translatedBytes, StandardCopyOption.REPLACE_EXISTING);
                            }
                        } else {
                            Files.copy(embeddedStreamTranslator.translate(tis, metadata), translatedBytes, StandardCopyOption.REPLACE_EXISTING);
                        }
                        try (TikaInputStream translated = TikaInputStream.get(translatedBytes)) {
                            digester.digest(translated, metadata, context);
                        }
                    }
                } else {
                    digester.digest(tis, metadata, context);
                }
                tis.reset();
                String digest;
                try {
                    digest = new DigestIdentifier(algorithm, Charset.defaultCharset()).generateForEmbed(embed);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
                if (documentCallback(metadata, digest, tis)) return;
                this.documentStack.add(embed);
                super.delegateParsing(tis, handler, metadata);
            } finally {
                this.documentStack.removeLast();
            }
        }

        protected File writeFile(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            File embedded = getEmbeddedPath(this.artifactPath, digest).toFile();
            Files.createDirectories(Paths.get(embedded.getParent()));
            try (FileOutputStream embeddedOutputStream = new FileOutputStream(embedded);
                 FileOutputStream metadataOutputStream = new FileOutputStream(embedded + ".json")) {
                int nbTmpBytesRead;
                for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = tis.read(tmp)) > 0; ) {
                    embeddedOutputStream.write(tmp, 0, nbTmpBytesRead); // streams in a file (using 8K memory buffer)
                }
                metadataOutputStream.write(new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
            }
            tis.reset();
            return embedded;
        }
    }

    static class DigestAllEmbeddedDocumentExtractor extends DigestEmbeddedDocumentExtractor {
        DigestAllEmbeddedDocumentExtractor(TikaDocument document, ParseContext context, DigestingParser.Digester digester, String algorithm, Path artifactPath, Function<TikaDocument, ModuleDescriptor.Version> tikaVersionSupplier) {
            super(document, context, digester, algorithm, artifactPath, tikaVersionSupplier);
        }

        @Override
        protected boolean documentCallback(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            writeFile(metadata, digest, tis);
            return false;
        }
    }

    static class DigestEmbeddedDocumentFileExtractor extends DigestEmbeddedDocumentExtractor {
        private final String digestToFind;
        private TikaDocumentSource document;

        private DigestEmbeddedDocumentFileExtractor(final TikaDocument rootDocument, final String digestToFind, ParseContext context, DigestingParser.Digester digester, String algorithm, Path artifactDir, Function<TikaDocument, ModuleDescriptor.Version> tikaVersionSupplier) {
            super(rootDocument, context, digester, algorithm, artifactDir, tikaVersionSupplier);
            this.digestToFind = digestToFind;
        }

        @Override
        protected boolean documentCallback(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            if (digestToFind.equals(digest)) {
                Supplier<InputStream> inputStreamSupplier = getInputStreamSupplier(metadata, digest, tis);
                this.document = new TikaDocumentSource(metadata, inputStreamSupplier);
                return true;
            }
            return false;
        }

        protected Supplier<InputStream> getInputStreamSupplier(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            File embedded = writeFile(metadata, digest, tis);
            return getFileInputStream(embedded);
        }

        static Supplier<InputStream> getFileInputStream(File embedded) {
            return () -> {
                try {
                    return new FileInputStream(embedded);
                } catch (FileNotFoundException e) {
                    throw new IllegalStateException("cannot find embedded file", e);
                }
            };
        }

        public TikaDocumentSource getDocument() {
            return ofNullable(document).orElseThrow(() ->
                    new ContentNotFoundException(documentStack.get(0).getPath().toString(), digestToFind)
            );
        }
    }

    static class DigestEmbeddedDocumentMemoryExtractor extends DigestEmbeddedDocumentFileExtractor {
        DigestEmbeddedDocumentMemoryExtractor(TikaDocument rootDocument, String digestToFind, ParseContext context, DigestingParser.Digester digester, String algorithm, Function<TikaDocument, ModuleDescriptor.Version> tikaVersionSupplier) {
            super(rootDocument, digestToFind, context, digester, algorithm, null, tikaVersionSupplier);
        }

        @Override
        protected Supplier<InputStream> getInputStreamSupplier(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nbTmpBytesRead;
            for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = tis.read(tmp)) > 0; ) {
                buffer.write(tmp, 0, nbTmpBytesRead); // uses the memory
            }
            return () -> new ByteArrayInputStream(buffer.toByteArray());
        }
    }

    private Parser createParserWithoutOCR() {
        try {
            return new AutoDetectParser(new TikaConfig(new ByteArrayInputStream((
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                            "<properties><parsers><parser class=\"org.apache.tika.parser.DefaultParser\">" +
                            "<parser-exclude class=\"org.apache.tika.parser.ocr.TesseractOCRParser\"/>" +
                            "</parser></parsers></properties>").getBytes())));
        } catch (TikaException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }
    public static class ContentNotFoundException extends NullPointerException {
        public ContentNotFoundException(String rootId, String embedId) {
            super(String.format("<%s> embedded document not found in root document <%s>", embedId, rootId));
        }

    }
}
