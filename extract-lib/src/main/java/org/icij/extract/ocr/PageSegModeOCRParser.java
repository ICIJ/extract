package org.icij.extract.ocr;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * Reads an image with each tesseract page segmentation mode in turn and keeps the first read that finds
 * text, so an image without a page layout is not left blank by mode 1.
 */
public class PageSegModeOCRParser implements Parser {
    /** Page segmentation mode of the kept read. Set by this parser, then by {@link AutoLanguageOCRParser} on retry. */
    public static final Property OCR_PAGE_SEG_MODE = Property.externalInteger("ocr:page_seg_mode");

    private final Parser delegate;
    private final List<String> modes;

    public PageSegModeOCRParser(Parser delegate, List<String> modes) {
        this.delegate = delegate;
        this.modes = modes;
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return delegate.getSupportedTypes(context);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        TesseractOCRConfig callerConfig = context.get(TesseractOCRConfig.class);
        restoringCallerConfig(context, callerConfig, () -> readStream(stream, handler, metadata, context));
    }

    // Every read swaps its own config into the shared context, so the caller's is put back even when a read fails.
    private static void restoringCallerConfig(ParseContext context, TesseractOCRConfig callerConfig, OcrStep step)
            throws IOException, SAXException, TikaException {
        try {
            step.run();
        } finally {
            context.set(TesseractOCRConfig.class, callerConfig);
        }
    }

    private void readStream(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        TesseractOCRConfig baseConfig = context.get(TesseractOCRConfig.class, new TesseractOCRConfig());
        try (TemporaryResources temporaryResources = new TemporaryResources()) {
            Path image = TikaInputStream.get(stream, temporaryResources, metadata).getPath();
            readImage(new ImageRead(delegate, image, metadata, context), baseConfig, handler);
        }
    }

    private void readImage(ImageRead read, TesseractOCRConfig baseConfig, ContentHandler handler)
            throws IOException, SAXException, TikaException {
        if (AutoLanguageOCRParser.readsAsIs(baseConfig, Files.size(read.image()))) {
            read.run(baseConfig, handler);
            return;
        }
        readWithFirstModeFindingText(read, baseConfig, handler);
    }

    private void readWithFirstModeFindingText(ImageRead read, TesseractOCRConfig baseConfig, ContentHandler handler)
            throws IOException, SAXException, TikaException {
        for (String mode : modes) {
            if (readAndKeepIfFinal(read, baseConfig, mode, handler)) {
                return;
            }
        }
    }

    private boolean readAndKeepIfFinal(ImageRead read, TesseractOCRConfig baseConfig, String mode,
                                       ContentHandler handler) throws IOException, SAXException, TikaException {
        SaxRecorder recorder = readWithMode(read, baseConfig, mode);
        return keepReadIfFinal(mode, recorder, handler, read.metadata());
    }

    private static SaxRecorder readWithMode(ImageRead read, TesseractOCRConfig baseConfig, String mode)
            throws IOException, SAXException, TikaException {
        TesseractOCRConfig config = SerializationUtils.clone(baseConfig);
        config.setPageSegMode(mode);
        SaxRecorder recorder = new SaxRecorder();
        read.run(config, recorder);
        return recorder;
    }

    private boolean isLastMode(String mode) {
        return mode.equals(modes.get(modes.size() - 1));
    }

    // The last mode is kept even when blank, so a blank image still produces a document.
    private boolean keepReadIfFinal(String mode, SaxRecorder recorder, ContentHandler handler, Metadata metadata)
            throws SAXException {
        if (!recorder.hasText() && !isLastMode(mode)) {
            return false;
        }
        keepRead(mode, recorder, handler, metadata);
        return true;
    }

    private static void keepRead(String mode, SaxRecorder recorder, ContentHandler handler, Metadata metadata)
            throws SAXException {
        recorder.replayTo(handler);
        metadata.set(OCR_PAGE_SEG_MODE, Integer.parseInt(mode));
    }

    private interface OcrStep {
        void run() throws IOException, SAXException, TikaException;
    }
}
