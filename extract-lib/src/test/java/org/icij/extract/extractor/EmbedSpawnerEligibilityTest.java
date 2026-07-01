package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.junit.Test;
import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerEligibilityTest {
    private Metadata image() {
        Metadata m = new Metadata();
        m.set(Metadata.CONTENT_TYPE, "image/png");
        return m;
    }

    @Test public void testImageLeafIsEligible() {
        assertThat(EmbedSpawner.isOcrEligible(image(), 0L)).isTrue();
    }

    @Test public void testNonImageIsNotEligible() {
        Metadata m = new Metadata();
        m.set(Metadata.CONTENT_TYPE, "application/pdf");
        assertThat(EmbedSpawner.isOcrEligible(m, 0L)).isFalse();
    }

    @Test public void testInlineImageIsNotEligible() {
        Metadata m = image();
        m.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE,
              TikaCoreProperties.EmbeddedResourceType.INLINE.toString());
        assertThat(EmbedSpawner.isOcrEligible(m, 0L)).isFalse();
    }

    @Test public void testImageBelowMinBytesIsNotEligible() {
        Metadata m = image();
        m.set(Metadata.CONTENT_LENGTH, "100");
        assertThat(EmbedSpawner.isOcrEligible(m, 4096L)).isFalse();
    }

    @Test public void testImageAtMinBytesIsEligible() {
        Metadata m = image();
        m.set(Metadata.CONTENT_LENGTH, "4096");
        assertThat(EmbedSpawner.isOcrEligible(m, 4096L)).isTrue();
    }

    @Test public void testImageWithNonNumericLengthIsEligible() {
        Metadata m = image();
        m.set(Metadata.CONTENT_LENGTH, "unknown");
        assertThat(EmbedSpawner.isOcrEligible(m, 4096L)).isTrue();
    }

    @Test public void testDepthDisabledWhenMaxIsZero() {
        assertThat(EmbedSpawner.exceedsMaxEmbedDepth(9999, 0)).isFalse();
    }

    @Test public void testDepthDisabledWhenMaxIsNegative() {
        assertThat(EmbedSpawner.exceedsMaxEmbedDepth(9999, -1)).isFalse();
    }

    @Test public void testDepthWithinLimitIsAllowed() {
        // stack size 20 with max 20: the boundary embed is allowed.
        assertThat(EmbedSpawner.exceedsMaxEmbedDepth(20, 20)).isFalse();
    }

    @Test public void testDepthBeyondLimitIsRefused() {
        // stack size 21 with max 20: the child at depth 21 is refused.
        assertThat(EmbedSpawner.exceedsMaxEmbedDepth(21, 20)).isTrue();
    }

    @Test public void testDepthOneBelowLimitIsAllowed() {
        assertThat(EmbedSpawner.exceedsMaxEmbedDepth(1, 20)).isFalse();
    }
}
