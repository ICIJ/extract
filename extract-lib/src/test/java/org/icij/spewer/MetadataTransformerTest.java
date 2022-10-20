package org.icij.spewer;

import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.fest.assertions.MapAssert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;


public class MetadataTransformerTest {
    @Test
    public void test_add_creation_date_metadata() throws IOException {
        Metadata metadata = new Metadata() ;
        LocalDateTime date = LocalDateTime.now();
        metadata.add(DublinCore.CREATED, DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss uuuu", Locale.ENGLISH).format(date));
        Map<String, Object> hashMetadata = new HashMap<>();


        new MetadataTransformer(metadata, new FieldNames()).transform(
                new Spewer.MapValueConsumer(hashMetadata), new Spewer.MapValuesConsumer(hashMetadata));

        assertThat(hashMetadata).includes(MapAssert.entry("tika_metadata_creation_date", hashMetadata.get("tika_metadata_dcterms_created")));
        assertThat(hashMetadata).includes(MapAssert.entry("tika_metadata_creation_date_iso8601", hashMetadata.get("tika_metadata_dcterms_created_iso8601")));
    }
}