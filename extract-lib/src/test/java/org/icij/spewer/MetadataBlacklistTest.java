package org.icij.spewer;

import org.junit.Test;
import static org.fest.assertions.Assertions.assertThat;

public class MetadataBlacklistTest {

    MetadataBlacklist metadataBlacklist = new MetadataBlacklist();

    @Test
    public void test_given_metadata_dcterms_is_ok() {
        assertThat(metadataBlacklist.ok("tika_metadata_dcterms_created")).isTrue();
    }

    @Test
    public void test_given_metadata_unknown_tags_are_not_ok() {
        assertThat(metadataBlacklist.ok("tika_metadata_unknown_tag_0x")).isFalse();
        assertThat(metadataBlacklist.ok("tika_metadata_unknown_tag_foo")).isFalse();
        assertThat(metadataBlacklist.ok("tika_metadata_unknown_tag_bar")).isFalse();
    }

    @Test
    public void test_given_range_is_not_ok() {
        assertThat(metadataBlacklist.ok("foo_1", "glob:foo_[123456789]")).isFalse();
    }
}
