package org.icij.spewer;

import org.junit.Test;
import static org.fest.assertions.Assertions.assertThat;

public class MetadataBlockListTest {

    MetadataBlockList metadataBlockList = new MetadataBlockList();

    @Test
    public void test_given_metadata_dcterms_is_ok() {
        assertThat(metadataBlockList.ok("tika_metadata_dcterms_created")).isTrue();
    }

    @Test
    public void test_given_metadata_unknown_tags_are_not_ok() {
        assertThat(metadataBlockList.ok("tika_metadata_unknown_tag_0x")).isFalse();
        assertThat(metadataBlockList.ok("tika_metadata_unknown_tag_foo")).isFalse();
        assertThat(metadataBlockList.ok("tika_metadata_unknown_tag_bar")).isFalse();
    }

    @Test
    public void test_given_range_is_not_ok() {
        assertThat(metadataBlockList.ok("foo_1", "glob:foo_[123456789]")).isFalse();
    }
}
