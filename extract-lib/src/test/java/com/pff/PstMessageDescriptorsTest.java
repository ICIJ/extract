package com.pff;

import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class PstMessageDescriptorsTest {

    @Test
    public void test_lists_all_normal_message_descriptors_in_testPST() throws Exception {
        String path = Paths.get(getClass().getResource("/documents/pst/testPST.pst").toURI()).toString();
        PSTFile pst = new PSTFile(path);
        try {
            List<Integer> ids = PstMessageDescriptors.normalMessageDescriptorIds(pst);
            // testPST.pst contains exactly 7 normal mail messages (0 orphans);
            // orphan/deleted-message recovery is validated separately by the
            // property-gated PstParityExternalTest against a real external PST.
            assertThat(ids).hasSize(7);
        } finally {
            pst.getFileHandle().close();
        }
    }
}
