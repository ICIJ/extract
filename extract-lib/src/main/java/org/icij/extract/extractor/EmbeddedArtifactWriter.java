package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.icij.spewer.MetadataTransformer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    // Copy already-spooled bytes into the raw payload (overwriting) and write the raw.json sidecar.
    static File write(final Path artifactPath, final String id, final Metadata metadata, final Path source) throws IOException {
        final File embedded = rawPath(artifactPath, id).toFile();
        Files.createDirectories(Paths.get(embedded.getParent()));
        Files.copy(source, embedded.toPath(), StandardCopyOption.REPLACE_EXISTING);
        try (FileOutputStream metadataOutputStream = new FileOutputStream(embedded + ".json")) {
            metadataOutputStream.write(new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
        }
        return embedded;
    }

    // Stream the embed bytes into the raw payload (8K buffer) and write the raw.json sidecar, then
    // reset the stream so the caller can reuse it. Mirrors EmbeddedDocumentExtractor's prior writeFile(tis).
    // Marks the stream itself before reading so reset() below is always valid, regardless of whether
    // the caller already marked it.
    static File write(final Path artifactPath, final String id, final Metadata metadata, final TikaInputStream tis) throws IOException {
        final File embedded = rawPath(artifactPath, id).toFile();
        Files.createDirectories(Paths.get(embedded.getParent()));
        tis.mark(0);
        try (FileOutputStream embeddedOutputStream = new FileOutputStream(embedded);
             FileOutputStream metadataOutputStream = new FileOutputStream(embedded + ".json")) {
            int nbTmpBytesRead;
            for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = tis.read(tmp)) > 0; ) {
                embeddedOutputStream.write(tmp, 0, nbTmpBytesRead);
            }
            metadataOutputStream.write(new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
        }
        tis.reset();
        return embedded;
    }
}
