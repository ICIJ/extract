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

    @Test public void testEmptyForOoxmlZipContainer() throws Exception {
        // OOXML documents (docx/xlsx/pptx) are ZIP containers parsed by Tika's OOXML parser, not
        // PackageParser: only the real embedded media becomes depth-1 embeds, so the central
        // directory count (which includes every internal part) must not be used as the estimate.
        Path docx = tmp.newFile("fake.docx").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(docx))) {
            zos.putNextEntry(new ZipEntry("[Content_Types].xml"));
            zos.write("<Types/>".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("word/document.xml"));
            zos.write("<document/>".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("word/media/image1.png"));
            zos.write(new byte[] {1, 2, 3});
            zos.closeEntry();
        }
        assertThat(ArchiveEntryCounter.countTopLevelEntries(docx).isPresent()).isFalse();
    }

    @Test public void testEmptyForOdfOrEpubZipContainer() throws Exception {
        // ODF (odt/ods/odp) and EPUB documents are also ZIP containers with a dedicated,
        // non-PackageParser Tika parser, signalled by a root "mimetype" entry that the EPUB/ODF
        // spec requires to be STORED (uncompressed).
        Path epub = tmp.newFile("fake.epub").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(epub))) {
            ZipEntry mimetype = new ZipEntry("mimetype");
            byte[] mimetypeBytes = "application/epub+zip".getBytes(StandardCharsets.UTF_8);
            mimetype.setMethod(ZipEntry.STORED);
            mimetype.setSize(mimetypeBytes.length);
            mimetype.setCompressedSize(mimetypeBytes.length);
            mimetype.setCrc(crc32(mimetypeBytes));
            zos.putNextEntry(mimetype);
            zos.write(mimetypeBytes);
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("META-INF/container.xml"));
            zos.write("<container/>".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("content.opf"));
            zos.write("<package/>".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }
        assertThat(ArchiveEntryCounter.countTopLevelEntries(epub).isPresent()).isFalse();
    }

    @Test public void testCountsZipWithDeflatedRootFileNamedMimetype() throws Exception {
        // A genuine archive can happen to contain a root file literally named "mimetype" that is
        // (as is typical) DEFLATED, not STORED. That must NOT be misclassified as an EPUB/ODF
        // container: it should still be counted normally. This is the regression the fix closes.
        Path zip = tmp.newFile("genuine.zip").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zip))) {
            ZipEntry mimetype = new ZipEntry("mimetype");
            mimetype.setMethod(ZipEntry.DEFLATED);
            zos.putNextEntry(mimetype);
            zos.write("just a coincidentally named file".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("readme.txt"));
            zos.write("hello".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("data.bin"));
            zos.write(new byte[] {1, 2, 3});
            zos.closeEntry();
        }
        OptionalLong result = ArchiveEntryCounter.countTopLevelEntries(zip);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.getAsLong()).isEqualTo(3L);
    }

    private static long crc32(final byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return crc.getValue();
    }
}
