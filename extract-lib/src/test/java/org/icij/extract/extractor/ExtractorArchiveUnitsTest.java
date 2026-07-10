package org.icij.extract.extractor;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractorArchiveUnitsTest {
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Test public void testApplyArchiveUnitCountSetsZipEntryTotal() throws Exception {
        Path zip = tmp.newFile("a.zip").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zip))) {
            for (int i = 0; i < 4; i++) {
                zos.putNextEntry(new ZipEntry("f" + i + ".txt"));
                zos.write(("v" + i).getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        ExtractionProgress progress = new ExtractionProgress(zip, 0L);
        new Extractor().applyArchiveUnitCount(zip, progress);
        assertThat(progress.expectedUnits()).isEqualTo(4L);
    }

    @Test public void testApplyArchiveUnitCountLeavesNonArchiveUnknown() throws Exception {
        Path txt = tmp.newFile("plain.txt").toPath();
        Files.write(txt, "hi".getBytes(StandardCharsets.UTF_8));
        ExtractionProgress progress = new ExtractionProgress(txt, 0L);
        new Extractor().applyArchiveUnitCount(txt, progress);
        assertThat(progress.expectedUnits()).isEqualTo(-1L);
    }

    @Test public void testApplyArchiveUnitCountLeavesOoxmlContainerUnknown() throws Exception {
        // docx/xlsx/pptx are ZIP containers parsed by Tika's OOXML parser, not PackageParser, so
        // the internal ZIP parts are not depth-1 embeds: pre-counting them would give a wrong
        // expectedUnits denominator. Must fall back to the count-only heartbeat.
        Path docx = tmp.newFile("fake.docx").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(docx))) {
            zos.putNextEntry(new ZipEntry("[Content_Types].xml"));
            zos.write("<Types/>".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("word/document.xml"));
            zos.write("<document/>".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }
        ExtractionProgress progress = new ExtractionProgress(docx, 0L);
        new Extractor().applyArchiveUnitCount(docx, progress);
        assertThat(progress.expectedUnits()).isEqualTo(-1L);
    }
}
