package org.icij.extract.cleaner;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ContentCleaner implements Cleaner {
    private final List<Cleaner> cleanerList;
    Detector detector = new DefaultDetector();

    public ContentCleaner(List<Cleaner> cleanerList) {
        this.cleanerList = cleanerList;
    }

    @Override
    public Set<MediaType> getSupportedTypes(CleanContext context) {
        Set<MediaType> mediaTypes = new HashSet<>();
        mediaTypes.addAll(MediaType.set("application/pdf"));
        mediaTypes.addAll(MediaType.set("application/msword"));
        return mediaTypes;
    }

    @Override
    public void clean(InputStream stream, DocumentSource documentSource, Metadata metadata, CleanContext context) throws IOException {
        try (TikaInputStream tis = TikaInputStream.get(new CloseShieldInputStream(stream))) {
            MediaType type = detector.detect(tis, metadata);

            if (getSupportedTypes(context).contains(type)) {
                Cleaner cleaner = getCleaners(context).get(type);
                cleaner.clean(tis, documentSource, metadata, context);
            } else {
                IOUtils.copy(tis, documentSource.getOutputStream());
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
