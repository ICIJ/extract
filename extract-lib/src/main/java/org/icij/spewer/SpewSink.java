package org.icij.spewer;

/**
 * Streaming spew callback used by the parse side to push embeds to a consumer as they are produced.
 *
 * <p>{@link #promise()} is called SYNCHRONOUSLY on the parse thread for every embed that is destined
 * to be spewed (both the synchronous and the deferred-OCR paths), at the moment the embed node is
 * created. {@link #ready(SpewItem)} is called when that embed's extracted text is fully buffered:
 * on the parse thread for synchronous embeds, and on the OCR completion thread for deferred embeds.
 * The promise/ready split lets the consumer know how many embeds to expect before the parse ends,
 * so it can wait for every deferred OCR to finish without a separate join.
 */
public interface SpewSink {
    /** Register that one more embed will eventually be {@link #ready(SpewItem)}. Parse thread only. */
    void promise();

    /** Hand a ready-to-write embed to the consumer. */
    void ready(SpewItem item);
}
