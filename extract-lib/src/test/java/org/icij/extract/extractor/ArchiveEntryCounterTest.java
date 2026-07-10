package org.icij.extract.extractor;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.fest.assertions.Assertions.assertThat;

public class ArchiveEntryCounterTest {
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    private Path zipWith(int files) throws Exception {
        Path zip = tmp.newFile("a.zip").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zip))) {
            zos.putNextEntry(new ZipEntry("dir/"));        // directory entry: must NOT be counted
            zos.closeEntry();
            for (int i = 0; i < files; i++) {
                zos.putNextEntry(new ZipEntry("dir/f" + i + ".txt"));
                zos.write(("hello" + i).getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        return zip;
    }

    @Test public void testCountsZipNonDirectoryEntries() throws Exception {
        assertThat(ArchiveEntryCounter.countTopLevelEntries(zipWith(3)).getAsLong()).isEqualTo(3L);
    }

    @Test public void testCounts7zNonDirectoryEntries() throws Exception {
        Path sevenZ = tmp.newFile("a.7z").toPath();
        try (SevenZOutputFile out = new SevenZOutputFile(sevenZ.toFile())) {
            for (int i = 0; i < 2; i++) {
                Path src = tmp.newFile("s" + i + ".txt").toPath();
                Files.write(src, ("x" + i).getBytes(StandardCharsets.UTF_8));
                SevenZArchiveEntry e = out.createArchiveEntry(src.toFile(), "s" + i + ".txt");
                out.putArchiveEntry(e);
                out.write(Files.readAllBytes(src));
                out.closeArchiveEntry();
            }
        }
        assertThat(ArchiveEntryCounter.countTopLevelEntries(sevenZ).getAsLong()).isEqualTo(2L);
    }

    @Test public void testEmptyForNonArchive() throws Exception {
        Path txt = tmp.newFile("plain.txt").toPath();
        Files.write(txt, "not an archive".getBytes(StandardCharsets.UTF_8));
        assertThat(ArchiveEntryCounter.countTopLevelEntries(txt).isPresent()).isFalse();
    }

    @Test public void testEmptyForCorruptZipMagic() throws Exception {
        Path fake = tmp.newFile("fake.zip").toPath();
        Files.write(fake, new byte[] {'P','K',3,4, 0,0,0,0}); // ZIP magic but truncated/garbage
        assertThat(ArchiveEntryCounter.countTopLevelEntries(fake).isPresent()).isFalse();
    }
}
