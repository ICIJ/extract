package org.icij.extract.extractor;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.OptionalLong;

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
            final byte[] magic = readMagic(path, 6);
            if (isZip(magic)) {
                return countZip(path);
            }
            if (isSevenZ(magic)) {
                return countSevenZ(path);
            }
            return OptionalLong.empty();
        } catch (final Throwable t) {
            // Never let a pre-count failure affect extraction.
            logger.debug("archive pre-count skipped for {}: {}", path, t.toString());
            return OptionalLong.empty();
        }
    }

    private static byte[] readMagic(final Path path, final int n) throws IOException {
        final byte[] buf = new byte[n];
        try (InputStream in = Files.newInputStream(path)) {
            int off = 0, r;
            while (off < n && (r = in.read(buf, off, n - off)) != -1) {
                off += r;
            }
        }
        return buf;
    }

    // Local file header "PK\x03\x04", empty archive "PK\x05\x06", spanned "PK\x07\x08".
    private static boolean isZip(final byte[] m) {
        return m.length >= 4 && m[0] == 'P' && m[1] == 'K'
                && (m[2] == 3 || m[2] == 5 || m[2] == 7)
                && (m[3] == 4 || m[3] == 6 || m[3] == 8);
    }

    // 7z signature: 37 7A BC AF 27 1C
    private static boolean isSevenZ(final byte[] m) {
        return m.length >= 6 && (m[0] & 0xFF) == 0x37 && (m[1] & 0xFF) == 0x7A
                && (m[2] & 0xFF) == 0xBC && (m[3] & 0xFF) == 0xAF
                && (m[4] & 0xFF) == 0x27 && (m[5] & 0xFF) == 0x1C;
    }

    private static OptionalLong countZip(final Path path) throws IOException {
        // ZipFile reads only the central directory; no entry data is decompressed.
        try (ZipFile zip = ZipFile.builder().setPath(path).get()) {
            long count = 0;
            final Enumeration<ZipArchiveEntry> entries = zip.getEntries();
            while (entries.hasMoreElements()) {
                if (!entries.nextElement().isDirectory()) {
                    count++;
                }
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
