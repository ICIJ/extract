package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.icij.spewer.MetadataTransformer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

// Single owner of the content-addressed raw layout for an embedded document's bytes.
// Both writers (the streaming EmbedSpawner and the on-demand EmbeddedDocumentExtractor) go
// through here so their on-disk layout, sidecar format and key can never drift apart.
// The layout is: payload at getEmbeddedPath(artifactPath, id)/raw and sidecar at raw.json,
// keyed by the embed id (NOT the raw file digest), so a manifest and the serving layer that
// resolve by id find the bytes this class writes.
class EmbeddedArtifactWriter {

    private EmbeddedArtifactWriter() {}

    // artifactPath/<id[0:2]>/<id[2:4]>/<id>/raw
    static Path rawPath(final Path artifactPath, final String id) {
        return ArtifactUtils.getEmbeddedPath(artifactPath, id).resolve("raw");
    }

    // Copy already-spooled bytes into the raw payload and write the raw.json sidecar, both via
    // write-to-temp-then-atomic-move so concurrent writers (e.g. two ARTIFACT workers
    // re-extracting the same root under parallelism > 1) never race on the final path: ids are
    // content-addressed, so concurrent writers always write identical bytes and atomic
    // last-writer-wins is correct. No reader can ever observe a partial raw or sidecar, and a
    // crash mid-write leaves only an orphaned temp file, never a truncated cache entry.
    static File write(final Path artifactPath, final String id, final Metadata metadata, final Path source) throws IOException {
        final Path embedded = rawPath(artifactPath, id);
        final Path dir = embedded.getParent();
        Files.createDirectories(dir);

        final Path rawTmp = Files.createTempFile(dir, "raw", ".tmp");
        final Path jsonTmp = Files.createTempFile(dir, "raw", ".json.tmp");
        try {
            Files.copy(source, rawTmp, StandardCopyOption.REPLACE_EXISTING);
            Files.write(jsonTmp, new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
            atomicMove(rawTmp, embedded);
            atomicMove(jsonTmp, sidecarOf(embedded));
        } finally {
            Files.deleteIfExists(rawTmp);
            Files.deleteIfExists(jsonTmp);
        }
        return embedded.toFile();
    }

    // Stream the embed bytes into the raw payload (8K buffer) via the same temp-then-atomic-move
    // as the Path overload, write the raw.json sidecar, then reset the stream so the caller can
    // reuse it. Mirrors EmbeddedDocumentExtractor's prior writeFile(tis).
    // Marks the stream itself before reading so reset() below is always valid, regardless of whether
    // the caller already marked it. Callers pass a file-backed TikaInputStream (spooled to disk), for
    // which mark/reset re-seeks the file and the readlimit is ignored; the 0 readlimit is therefore safe
    // here and would only be a concern for a purely in-memory, non-file-backed stream, which never reaches
    // this overload (EmbeddedDocumentExtractor's callers spool first; EmbedSpawner uses the Path overload).
    static File write(final Path artifactPath, final String id, final Metadata metadata, final TikaInputStream tis) throws IOException {
        final Path embedded = rawPath(artifactPath, id);
        final Path dir = embedded.getParent();
        Files.createDirectories(dir);

        tis.mark(0);
        final Path rawTmp = Files.createTempFile(dir, "raw", ".tmp");
        final Path jsonTmp = Files.createTempFile(dir, "raw", ".json.tmp");
        try {
            try (OutputStream embeddedOutputStream = Files.newOutputStream(rawTmp)) {
                int nbTmpBytesRead;
                for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = tis.read(tmp)) > 0; ) {
                    embeddedOutputStream.write(tmp, 0, nbTmpBytesRead);
                }
            }
            Files.write(jsonTmp, new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
            atomicMove(rawTmp, embedded);
            atomicMove(jsonTmp, sidecarOf(embedded));
        } finally {
            Files.deleteIfExists(rawTmp);
            Files.deleteIfExists(jsonTmp);
        }
        // Only reached once the new bytes are fully written and moved into place; on any failure
        // above, the exception propagates and the stream is left unreset (same as before this fix).
        tis.reset();
        return embedded.toFile();
    }

    private static Path sidecarOf(Path embedded) {
        return embedded.resolveSibling(embedded.getFileName() + ".json");
    }

    // Moves the temp file into place atomically (same directory as the target, so always the
    // same filesystem -- ATOMIC_MOVE applies). Falls back to a plain replace-existing move on the
    // rare filesystem that doesn't support atomic rename; still far better than the in-place
    // copy/truncate this replaces, just without the atomicity guarantee.
    private static void atomicMove(final Path tmp, final Path target) throws IOException {
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
