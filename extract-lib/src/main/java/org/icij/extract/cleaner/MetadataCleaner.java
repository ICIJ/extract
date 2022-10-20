package org.icij.extract.cleaner;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class MetadataCleaner {
    private final ContentCleaner cleaner;

    public MetadataCleaner() {
        this.cleaner = new ContentCleaner(asList(new PdfMetadataCleaner(), new OfficeWordMetadataCleaner()));
    }

    public DocumentSource clean(Path document) throws IOException {
        return clean(new FileInputStream(document.toFile()));
    }

    public DocumentSource clean(InputStream inputStream) throws IOException {
        CleanContext context = new CleanContext();
        context.set(Cleaner.class, cleaner);
        final DocumentSource documentSource = new DocumentSource();
        Metadata metadata = new Metadata();

        cleaner.clean(inputStream, documentSource, metadata, context);

        return documentSource;
    }

    public static class PdfMetadataCleaner implements Cleaner {
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

    public static class OfficeWordMetadataCleaner implements Cleaner {
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
            List<Method> methods = Arrays.stream(summaryInformation.getClass().getMethods()).filter(method -> method.getName().startsWith("remove")).collect(Collectors.toList());
            methods.forEach(method -> {
                try {
                    method.invoke(summaryInformation);
                } catch (IllegalAccessException|InvocationTargetException e) {
                    throw new InternalCleanerException(e) ;
                }
            });
        }
    }

    static class InternalCleanerException extends RuntimeException {
        public InternalCleanerException(Exception e) {
            super(e);
        }
    }
}
