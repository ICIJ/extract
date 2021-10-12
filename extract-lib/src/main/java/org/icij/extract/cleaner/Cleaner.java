package org.icij.extract.cleaner;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public interface Cleaner {
    Set<MediaType> getSupportedTypes(CleanContext context);
    void clean(InputStream stream, DocumentSource documentSource, Metadata metadata, CleanContext context) throws IOException;
}
