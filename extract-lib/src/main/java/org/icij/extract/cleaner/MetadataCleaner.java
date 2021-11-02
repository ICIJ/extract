package org.icij.extract.cleaner;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.microsoft.OfficeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class MetadataCleaner {
    private final ContentCleaner cleaner;

    public MetadataCleaner() {
        this.cleaner = new ContentCleaner(asList(new PdfMetadataCleaner(), new OfficeMetadataCleaner()));
    }

    public DocumentSource clean(Path document) throws IOException {
        CleanContext context = new CleanContext();
        context.set(Cleaner.class, cleaner);
        final DocumentSource documentSource = new DocumentSource();
        Metadata metadata = new Metadata();

        cleaner.clean(new FileInputStream(document.toFile()), documentSource, metadata, context);

        return documentSource;
    }

    static class ContentCleaner implements Cleaner {
        private final List<Cleaner> cleanerList;
        Detector detector = new DefaultDetector();

        public ContentCleaner(List<Cleaner> cleanerList) {
            this.cleanerList = cleanerList;
        }

        @Override
        public Set<MediaType> getSupportedTypes(CleanContext context) {
            Set<MediaType> mediaTypes = new HashSet<>();
            mediaTypes.addAll(MediaType.set("application/pdf"));
            mediaTypes.addAll(Arrays.stream(OfficeParser.POIFSDocumentType.values()).map(OfficeParser.POIFSDocumentType::getType).collect(Collectors.toSet()));
            return mediaTypes;
        }

        @Override
        public void clean(InputStream stream, DocumentSource documentSource, Metadata metadata, CleanContext context) throws IOException {
            try (TikaInputStream tis = TikaInputStream.get(new CloseShieldInputStream(stream))) {
                MediaType type = detector.detect(tis, metadata);

                if (getSupportedTypes(context).contains(type)) {
                    Cleaner cleaner = getCleaners(context).get(type);
                    cleaner.clean(tis, documentSource, metadata, context);
                }
            }
        }

        public Map<MediaType, Cleaner> getCleaners(CleanContext context) {
            Map<MediaType, Cleaner> map = new HashMap<>();
            for (Cleaner cleaner : cleanerList) {
                for (MediaType type : cleaner.getSupportedTypes(context)) {
                    map.put(type, cleaner);
                }
            }
            return map;
        }
    }

    static class PdfMetadataCleaner implements Cleaner {
        @Override
        public Set<MediaType> getSupportedTypes(CleanContext context) {
            return MediaType.set("application/pdf");
        }

        @Override
        public void clean(InputStream stream, DocumentSource documentSource, Metadata metadata, CleanContext context) throws IOException {
            PDDocument document = PDDocument.load(stream);
            PDDocumentInformation information = document.getDocumentInformation();
            document.getDocumentCatalog().setMetadata(null);
            if (information != null) {
                document.setDocumentInformation(new PDDocumentInformation());
                document.save(documentSource.getOutputStream());
            }
            document.close();
        }
    }

    static class OfficeMetadataCleaner implements Cleaner {
        @Override
        public Set<MediaType> getSupportedTypes(CleanContext context) {
            return MediaType.set("application/msword");
        }

        @Override
        public void clean(InputStream stream, DocumentSource documentSource, Metadata metadata, CleanContext context) throws IOException {
            HWPFDocument document = new HWPFDocument(stream);
            removeSummaryInformationMetadata(document.getSummaryInformation());
            document.write(documentSource.getOutputStream());
        }

        private void removeSummaryInformationMetadata(SummaryInformation summaryInformation) {
            summaryInformation.removeAuthor();
            summaryInformation.removeLastAuthor();
            summaryInformation.removeSubject();
            summaryInformation.removeComments();
            summaryInformation.removeCreateDateTime();
            summaryInformation.removeEditTime();
            summaryInformation.removeLastSaveDateTime();
            summaryInformation.removeApplicationName();
            summaryInformation.removeRevNumber();
            summaryInformation.removeLastPrinted();
            summaryInformation.removeKeywords();
        }
    }
}
