package com.pff;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Opt-in byte-identity check (milestone M1): runs only when a real OST and the matching libpff truth
 * files are supplied via system properties, otherwise self-skips so CI is unaffected.
 *
 * <pre>
 * mvn -q -pl extract-lib test -Dtest=OstByteIdentityValidationTest \
 *   -Dost.test.file=/path/to.ost \
 *   -Dost.silent.descriptorId=NNNN -Dost.silent.attIndex=0 -Dost.silent.truth=/tmp/truth-silent.bin \
 *   -Dost.throw.descriptorId=MMMM -Dost.throw.attIndex=0 -Dost.throw.truth=/tmp/truth-throw.bin
 * </pre>
 */
public class OstByteIdentityValidationTest {

    @Test
    public void silentClassAttachmentMatchesLibpffBytes() throws Exception {
        assertRecoveryMatchesTruth("ost.silent.descriptorId", "ost.silent.attIndex", "ost.silent.truth");
    }

    @Test
    public void decompressClassAttachmentMatchesLibpffBytes() throws Exception {
        assertRecoveryMatchesTruth("ost.throw.descriptorId", "ost.throw.attIndex", "ost.throw.truth");
    }

    private void assertRecoveryMatchesTruth(final String idProp, final String idxProp, final String truthProp)
            throws Exception {
        final String ostPath = System.getProperty("ost.test.file");
        final String descriptorId = System.getProperty(idProp);
        final String attIndex = System.getProperty(idxProp);
        final String truthPath = System.getProperty(truthProp);
        assumeTrue(ostPath != null && descriptorId != null && attIndex != null && truthPath != null);

        final PSTFile pstFile = new PSTFile(ostPath);
        try {
            final PSTObject object =
                    PSTObject.detectAndLoadPSTObject(pstFile, Long.parseLong(descriptorId));
            assertThat(object).isInstanceOf(PSTMessage.class);
            final PSTAttachment attachment = ((PSTMessage) object).getAttachment(Integer.parseInt(attIndex));

            final Optional<byte[]> recovered = OstCompressedBlockReader.recover(attachment);
            assertThat(recovered.isPresent()).isTrue();

            final byte[] truth = Files.readAllBytes(Paths.get(truthPath));
            assertThat(recovered.get().length).isEqualTo(truth.length);
            assertThat(sha256(recovered.get())).isEqualTo(sha256(truth));
        } finally {
            if (pstFile.getFileHandle() != null) {
                pstFile.getFileHandle().close();
            }
        }
    }

    private static String sha256(final byte[] data) throws Exception {
        final byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
        final StringBuilder sb = new StringBuilder();
        for (final byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
