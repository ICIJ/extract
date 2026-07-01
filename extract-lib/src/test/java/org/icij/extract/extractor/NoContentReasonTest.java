package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class NoContentReasonTest {

    @Test
    public void testKeyIsStable() {
        assertThat(NoContentReason.METADATA_KEY).isEqualTo("X-Extract:no-content-reason");
    }

    @Test
    public void testValues() {
        assertThat(NoContentReason.UNSUPPORTED_MEDIA_TYPE.value()).isEqualTo("unsupported-media-type");
        assertThat(NoContentReason.EMPTY_FILE.value()).isEqualTo("empty-file");
        assertThat(NoContentReason.ENCRYPTED.value()).isEqualTo("encrypted");
    }

    @Test
    public void testStampSetsSingleValue() {
        Metadata metadata = new Metadata();
        NoContentReason.stamp(metadata, NoContentReason.ENCRYPTED);
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("encrypted");
        assertThat(metadata.getValues(NoContentReason.METADATA_KEY).length).isEqualTo(1);
    }

    @Test
    public void testStampOverwrites() {
        Metadata metadata = new Metadata();
        NoContentReason.stamp(metadata, NoContentReason.EMPTY_FILE);
        NoContentReason.stamp(metadata, NoContentReason.UNSUPPORTED_MEDIA_TYPE);
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("unsupported-media-type");
        assertThat(metadata.getValues(NoContentReason.METADATA_KEY).length).isEqualTo(1);
    }
}
