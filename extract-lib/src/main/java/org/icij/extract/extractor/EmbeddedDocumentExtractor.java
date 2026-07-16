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
import org.apache.tika.parser.microsoft.pst.OutlookPSTParser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.icij.extract.parser.ResilientOutlookPSTParser;
import org.icij.spewer.MetadataTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedDocumentExtractor.class);
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
        this.parser = new DigestingParser(ocr ? withResilientPstParser() : createParserWithoutOCR(), digester, false);
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
                try {
                    return new TikaDocumentSource(MetadataTransformer.loadMetadata(new File(cachedFile + ".json")), DigestEmbeddedDocumentFileExtractor.getFileInputStream(cachedFile));
                } catch (IOException e) {
                    // N2: a missing/corrupt sidecar (partial write, crash, format drift) must not be
                    // a permanent failure -- the source is re-derivable from the root document, so
                    // treat this as a cache miss and fall through to the live-parse path below.
                    logger.debug("cache read failed for embedded document <{}>; falling back to live parse", embeddedDocumentDigest, e);
                }
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

    // Test-support only: exposes the constructed parser so a test can walk its sub-parser
    // tree and assert which concrete parsers are wired in (e.g. resilient vs stock Outlook
    // PST parser), without re-deriving the construction logic in the test itself.
    Parser getParser() {
        return parser;
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
            // Push right away, before any of the digesting/callback work below that can throw
            // (e.g. a per-message digest failure in a resilient walk). The matching finally
            // always pops exactly what this invocation pushed, so a failure here can never
            // underflow the stack and corrupt the walk for the next sibling embed.
            this.documentStack.add(embed);
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
                    // Parse FIRST, then freeze the id and write -- mirroring EmbedSpawner
                    // (parse-then-write). The sub-parse (e.g. PSTMailItemParser) enriches this
                    // embed's metadata with EMBEDDED_RELATIONSHIP_ID / resourceName / Content-Type,
                    // all of which DigestIdentifier folds into the id. Freezing the id BEFORE the
                    // sub-parse (as this used to) dropped those fields, so retrieval wrote bytes
                    // under ids the index never produced (OST-2). This embed's own content hash is
                    // set above and is NOT overwritten by the recursion (each child digests into its
                    // OWN metadata), so the frozen id = SHA(thisEmbedHash || parentId || relId ||
                    // name) matches the id EmbedSpawner froze after the same sub-parse at index time.
                    try (TikaInputStream parseStream = TikaInputStream.get(spooled)) {
                        super.delegateParsing(parseStream, handler, metadata);
                    }
                    String digest = embed.getId();
                    documentCallback(metadata, digest, spooled);
                    return;
                }

                // Spool the entry to a temp file so its bytes survive the sub-parse below (which
                // consumes the stream) and remain writable afterwards; mirrors EmbedSpawner, which
                // spools via tis.getPath() before delegateParsing and writes the same file after.
                // This also file-backs tis so the mark/reset digest below re-seeks the file.
                final Path spooled = tis.getPath();
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
                // Parse FIRST, then freeze the id and write from the spooled bytes -- see the
                // raw-branch note above. The sub-parse enriches EMBEDDED_RELATIONSHIP_ID /
                // resourceName / Content-Type that DigestIdentifier folds into the id, so freezing
                // the id after it makes retrieval ids match the index ids (OST-2). This embed's own
                // content hash is set above and survives the recursion (children digest into their
                // own metadata), so the composed id is unchanged from EmbedSpawner's.
                super.delegateParsing(tis, handler, metadata);
                String digest = embed.getId();
                documentCallback(metadata, digest, spooled);
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
            TikaConfig config = new TikaConfig(new ByteArrayInputStream((
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                            "<properties><parsers><parser class=\"org.apache.tika.parser.DefaultParser\">" +
                            "<parser-exclude class=\"org.apache.tika.parser.ocr.TesseractOCRParser\"/>" +
                            "</parser></parsers></properties>").getBytes()));
            return new AutoDetectParser(config.getDetector(), withResilientPstParser(config.getParser()));
        } catch (TikaException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }

    // Builds the default (OCR-enabled) base parser, with Tika's stock OutlookPSTParser swapped
    // for the resilient one -- the same swap Extractor applies at INDEX time (see
    // Extractor.replaceParser(OutlookPSTParser.class, ...)). Without it, ARTIFACT/download
    // re-extraction from a PST/OST uses the stock parser and fails (e.g. "OST 2013 support not
    // added yet") even though INDEX succeeded.
    private Parser withResilientPstParser() {
        return new AutoDetectParser(withResilientPstParser(TikaConfig.getDefaultConfig().getParser()));
    }

    // Applies the same swap to an already-built (non-AutoDetectParser) composite, such as
    // TikaConfig's DefaultParser. Must run on this inner composite, not on a constructed
    // AutoDetectParser: replaceParser flattens whatever composite it's given into a plain
    // CompositeParser, so applying it to an AutoDetectParser instance directly would discard its
    // detector/config wiring and break MIME-type auto-detection for every format, not just PST.
    private static Parser withResilientPstParser(Parser configParser) {
        return Extractor.replaceParser(configParser, OutlookPSTParser.class, p -> new ResilientOutlookPSTParser());
    }

    public static class ContentNotFoundException extends NullPointerException {
        public ContentNotFoundException(String rootId, String embedId) {
            super(String.format("<%s> embedded document not found in root document <%s>", embedId, rootId));
        }

    }
}
