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
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.icij.spewer.MetadataTransformer;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.lang.module.ModuleDescriptor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

public class EmbeddedDocumentExtractor {
    private final Parser parser;
    private final DigestingParser.Digester digester;
    private final String algorithm;
    private final Path artifactPath;
    private final boolean ocr;
    private static final ModuleDescriptor.Version TIKA_3_2_3 = ModuleDescriptor.Version.parse("3.2.3");

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
        this.parser = new DigestingParser(ocr ? new AutoDetectParser() : createParserWithoutOCR(), digester, false);
        this.digester = digester;
        this.artifactPath = artifactPath;
        this.algorithm = algorithm;
        this.ocr = ocr;
    }

    /**
     * Builds the parse context. When OCR is enabled we keep the OCR parser registered (rather
     * than excluding it as createParserWithoutOCR does), so OCR-routed image embeds are still
     * produced with the same identifiers as at index time, but we set skipOcr so Tesseract does
     * not actually run. Extracting an embedded *source* only needs the entry bytes and their
     * digest (which is computed before the recognition step would run), never the OCR text, so
     * skipping recognition reproduces the identical embedded document without the Tesseract cost.
     */
    private ParseContext newParseContext() {
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        if (ocr) {
            TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
            ocrConfig.setSkipOcr(true);
            context.set(TesseractOCRConfig.class, ocrConfig);
        }
        return context;
    }

    public void extractAll(final TikaDocument document) throws SAXException, TikaException, IOException {
        ofNullable(artifactPath).orElseThrow(() -> new IllegalStateException("cannot extract all embedded files in memory"));
        ParseContext context = newParseContext();
        ContentHandler handler = new BodyContentHandler(-1);

        DigestEmbeddedDocumentExtractor extractor = new DigestAllEmbeddedDocumentExtractor(document, context, digester, algorithm, artifactPath);
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, extractor);

        try (InputStream in = Files.newInputStream(document.getPath())) {
            parser.parse(in, handler, document.getMetadata(), context);
        }
    }

    public TikaDocumentSource extract(final TikaDocument rootDocument, final String embeddedDocumentDigest) throws SAXException, TikaException, IOException {
        if (artifactPath != null) {
            File cachedFile = getEmbeddedPath(artifactPath, embeddedDocumentDigest).toFile();
            if (cachedFile.exists()) {
                return new TikaDocumentSource(MetadataTransformer.loadMetadata(new File(cachedFile + ".json")), DigestEmbeddedDocumentFileExtractor.getFileInputStream(cachedFile));
            }
        }
        ParseContext context = newParseContext();
        ContentHandler handler = new BodyContentHandler(-1);

        DigestEmbeddedDocumentFileExtractor extractor = getExtractor(rootDocument, embeddedDocumentDigest, context, artifactPath);
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, extractor);

        try (InputStream in = Files.newInputStream(rootDocument.getPath())) {
            parser.parse(in, handler, rootDocument.getMetadata(), context);
        }

        return extractor.getDocument();
    }

    private DigestEmbeddedDocumentFileExtractor getExtractor(TikaDocument rootDocument, String embeddedDocumentDigest, ParseContext context, Path artifactPath) {
        if (artifactPath != null) {
            return new DigestEmbeddedDocumentFileExtractor(rootDocument, embeddedDocumentDigest, context, digester, algorithm, artifactPath);
        } else {
            return new DigestEmbeddedDocumentMemoryExtractor(rootDocument, embeddedDocumentDigest, context, digester, algorithm);
        }
    }

    static Path getEmbeddedPath(Path artifactPath, String digest) {
        return EmbeddedArtifactWriter.rawPath(artifactPath, digest);
    }

    private static abstract class DigestEmbeddedDocumentExtractor extends EmbedParser {
        private final DigestingParser.Digester digester;
        private final String algorithm;
        protected final Path artifactPath;
        protected final LinkedList<TikaDocument> documentStack = new LinkedList<>();
        protected final ModuleDescriptor.Version documentTikaVersion;
        private final EmbeddedStreamTranslator embeddedStreamTranslator = new DefaultEmbeddedStreamTranslator();


        DigestEmbeddedDocumentExtractor(TikaDocument document, ParseContext context, DigestingParser.Digester digester,
                                        String algorithm, Path artifactPath) {
            super(document, context);
            this.digester = digester;
            this.algorithm = algorithm;
            this.artifactPath = artifactPath;
            this.documentStack.add(document);
            this.documentTikaVersion = document.getTikaVersion();
        }

        protected abstract boolean documentCallback(Metadata metadata, String digest, TikaInputStream tis) throws IOException;
        protected abstract boolean documentCallback(Metadata metadata, String digest, Path spooled) throws IOException;

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
                // this if/else is coming from DigestingParser since 3.3.0 to fix issues with Microsoft OLE docs
                // see https://issues.apache.org/jira/browse/TIKA-4533
                boolean shouldTranslate = embeddedStreamTranslator.shouldTranslate(tis, metadata);
                boolean retroCompat = documentTikaVersion.compareTo(TIKA_3_2_3) <= 0;

                if (!(shouldTranslate && !retroCompat) && tis.getOpenContainer() == null) {
                    // Raw (non-translated) entry from a container such as a zip/tar archive.
                    // Spool the entry to a temp file once, then read independent file-backed
                    // streams for the digest, the document callback and the recursive parse.
                    // This avoids the in-memory mark()/reset() on the embedded stream, which
                    // fails with "Resetting to invalid mark" (surfaced as TIKA-198 from
                    // PackageParser) once an entry exceeds the digester's mark limit, and keeps
                    // the per-entry parse memory bounded regardless of entry size. (The memory
                    // extractor still buffers the single matched entry into a byte[] by design.)
                    final Path spooled = tis.getPath();
                    try (TikaInputStream digestStream = TikaInputStream.get(spooled)) {
                        digester.digest(digestStream, metadata, context);
                    }
                    // Cache the embed id now while metadata still holds the correct hash:
                    // super.delegateParsing below triggers DigestingParser which overwrites it.
                    String digest = embed.getId();
                    boolean found = documentCallback(metadata, digest, spooled);
                    this.documentStack.add(embed);
                    // Once the target embed is captured, skip recursing into it: its content is
                    // already saved and recursion would parse/OCR it needlessly. Combined with
                    // shouldParseEmbedded() returning false afterwards, this stops the walk early.
                    if (!found) {
                        try (TikaInputStream parseStream = TikaInputStream.get(spooled)) {
                            super.delegateParsing(parseStream, handler, metadata);
                        }
                    }
                    return;
                }

                tis.mark(0); // Marking the position before resetting
                if (shouldTranslate && !retroCompat) {
                    // Tika >= 3.3.0: hash the translated bytes so the digest matches the actual content
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
                    // retro-compat (<= 3.2.3) or no translation needed: hash raw bytes
                    digester.digest(tis, metadata, context);
                }
                tis.reset();
                // Force the embed ID to be cached now while metadata still holds the correct hash.
                // super.delegateParsing below triggers DigestingParser which overwrites the hash in metadata;
                // without pre-caching, lazy getId() on this embed would compute with the wrong hash
                // and break ID lookups for any child embeds that reference this embed as parent.
                String digest = embed.getId();
                boolean found = documentCallback(metadata, digest, tis);
                this.documentStack.add(embed);
                if (!found) {
                    super.delegateParsing(tis, handler, metadata);
                }
            } finally {
                this.documentStack.removeLast();
            }
        }

        protected File writeFile(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            return EmbeddedArtifactWriter.write(this.artifactPath, digest, metadata, tis);
        }

        protected File writeFile(Metadata metadata, String digest, Path source) throws IOException {
            return EmbeddedArtifactWriter.write(this.artifactPath, digest, metadata, source);
        }
    }

    static class DigestAllEmbeddedDocumentExtractor extends DigestEmbeddedDocumentExtractor {
        DigestAllEmbeddedDocumentExtractor(TikaDocument document, ParseContext context, DigestingParser.Digester digester, String algorithm, Path artifactPath) {
            super(document, context, digester, algorithm, artifactPath);
        }

        @Override
        protected boolean documentCallback(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            writeFile(metadata, digest, tis);
            return false;
        }

        @Override
        protected boolean documentCallback(Metadata metadata, String digest, Path spooled) throws IOException {
            writeFile(metadata, digest, spooled);
            return false;
        }
    }

    static class DigestEmbeddedDocumentFileExtractor extends DigestEmbeddedDocumentExtractor {
        private final String digestToFind;
        private TikaDocumentSource document;

        private DigestEmbeddedDocumentFileExtractor(final TikaDocument rootDocument, final String digestToFind, ParseContext context, DigestingParser.Digester digester, String algorithm, Path artifactDir) {
            super(rootDocument, context, digester, algorithm, artifactDir);
            this.digestToFind = digestToFind;
        }

        @Override
        public boolean shouldParseEmbedded(Metadata metadata) {
            // Stop descending once the target embed has been found. Tika checks this before
            // parsing each embedded entry, so returning false skips the remaining (potentially
            // OCR-heavy) entries instead of walking the whole archive. This hook is the clean
            // way to short-circuit: it needs no exception and works the same for every container.
            return document == null && super.shouldParseEmbedded(metadata);
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

        @Override
        protected boolean documentCallback(Metadata metadata, String digest, Path spooled) throws IOException {
            if (digestToFind.equals(digest)) {
                Supplier<InputStream> inputStreamSupplier = getInputStreamSupplier(metadata, digest, spooled);
                this.document = new TikaDocumentSource(metadata, inputStreamSupplier);
                return true;
            }
            return false;
        }

        protected Supplier<InputStream> getInputStreamSupplier(Metadata metadata, String digest, Path spooled) throws IOException {
            File embedded = writeFile(metadata, digest, spooled);
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
        DigestEmbeddedDocumentMemoryExtractor(TikaDocument rootDocument, String digestToFind, ParseContext context, DigestingParser.Digester digester, String algorithm) {
            super(rootDocument, digestToFind, context, digester, algorithm, null);
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

        @Override
        protected Supplier<InputStream> getInputStreamSupplier(Metadata metadata, String digest, Path spooled) throws IOException {
            byte[] bytes = Files.readAllBytes(spooled);
            return () -> new ByteArrayInputStream(bytes);
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
