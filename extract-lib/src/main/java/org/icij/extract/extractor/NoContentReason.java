package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;

/**
 * Records why a document has no extracted content, without treating it as a failure.
 *
 * A single metadata key ({@link #METADATA_KEY}) carries one of the values below, so a single index
 * filter can surface every document that was intentionally not text-extracted, with the reason
 * available to distinguish the cases. The document is still indexed with its metadata; only the
 * extracted body is empty.
 */
public enum NoContentReason {
    /** Non-empty file whose media type has no Tika parser (e.g. .pyc, .mo, web fonts, .wasm). */
    UNSUPPORTED_MEDIA_TYPE("unsupported-media-type"),
    /** Zero-byte file. */
    EMPTY_FILE("empty-file"),
    /** Password-protected document that could not be decrypted. */
    ENCRYPTED("encrypted");

    public static final String METADATA_KEY = "X-Extract:no-content-reason";

    private final String value;

    NoContentReason(final String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /** Stamp the reason onto the given metadata, replacing any previous value. */
    public static void stamp(final Metadata metadata, final NoContentReason reason) {
        metadata.set(METADATA_KEY, reason.value());
    }
}
