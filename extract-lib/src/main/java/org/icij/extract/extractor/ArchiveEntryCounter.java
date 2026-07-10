package org.icij.extract.extractor;

import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.OptionalLong;
import java.util.zip.ZipEntry;

/**
 * Cheaply counts the top-level entries of an archive whose format carries a catalog (ZIP central
 * directory, 7z header), without decompressing content. Used to give the progress heartbeat a
 * denominator. Any failure (unknown/unsupported format, corrupt or encrypted archive, no readable
 * file) yields {@link OptionalLong#empty()} so extraction proceeds with the count-only heartbeat.
 * Directory entries are excluded so the count matches the embeds Tika's PackageParser emits.
 */
public final class ArchiveEntryCounter {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveEntryCounter.class);

    private ArchiveEntryCounter() {}

    public static OptionalLong countTopLevelEntries(final Path path) {
        try {
            final String type;
            try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
                type = ArchiveStreamFactory.detect(in);
            }
            if (ArchiveStreamFactory.ZIP.equals(type)) {
                return countZip(path);
            }
            if (ArchiveStreamFactory.SEVEN_Z.equals(type)) {
                return countSevenZ(path);
            }
            return OptionalLong.empty();
        } catch (final Throwable t) {
            // Never let a pre-count failure affect extraction.
            logger.debug("archive pre-count skipped for {}: {}", path, t.toString());
            return OptionalLong.empty();
        }
    }

    private static OptionalLong countZip(final Path path) throws IOException {
        // ZipFile reads only the central directory; no entry data is decompressed.
        try (ZipFile zip = ZipFile.builder().setPath(path).get()) {
            long count = 0;
            boolean isContainerDocument = false;
            final Enumeration<ZipArchiveEntry> entries = zip.getEntries();
            while (entries.hasMoreElements()) {
                final ZipArchiveEntry entry = entries.nextElement();
                final String name = entry.getName();
                // OOXML (docx/xlsx/pptx) and ODF/EPUB (odt/ods/odp/epub) are ZIP containers, but
                // Tika parses them with the dedicated OOXML/ODF parsers, not PackageParser. Those
                // parsers emit only the real embedded media as depth-1 embeds (a handful), while
                // this central-directory count would otherwise count every internal ZIP part
                // (dozens), producing a percentage that never reaches 100%. Fall back to the
                // count-only heartbeat for these.
                // OOXML marks itself with a root "[Content_Types].xml"; ODF/EPUB with a root "mimetype"
                // entry that the spec requires to be STORED (uncompressed). Requiring STORED for the
                // mimetype case avoids misclassifying a genuine archive that merely contains a
                // (typically deflated) file named "mimetype".
                if ("[Content_Types].xml".equals(name)
                        || ("mimetype".equals(name) && entry.getMethod() == ZipEntry.STORED)) {
                    isContainerDocument = true;
                }
                if (!entry.isDirectory()) {
                    count++;
                }
            }
            if (isContainerDocument) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(count);
        }
    }

    private static OptionalLong countSevenZ(final Path path) throws IOException {
        try (SevenZFile sevenZ = SevenZFile.builder().setPath(path).get()) {
            long count = 0;
            for (final SevenZArchiveEntry e : sevenZ.getEntries()) {
                if (!e.isDirectory()) {
                    count++;
                }
            }
            return OptionalLong.of(count);
        }
    }
}
