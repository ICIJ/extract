package org.icij.extract.parser;

/**
 * Skip policy for RESUMABLE extraction of a single large PST/OST mailbox.
 *
 * <p>Carried through the Tika {@link org.apache.tika.parser.ParseContext} (like
 * {@link PstFanoutConfig}) and consulted by {@link ResilientOutlookPSTParser} before it emits each
 * mail message. When a caller supplies a policy that reports a unit as already done, the parser
 * skips re-parsing and re-emitting that unit; a restart therefore does not reparse a giant mailbox
 * from byte 0.
 *
 * <p>The persistence of "which units are done" is deliberately NOT here: only the consumer of the
 * emitted documents (Datashare) knows when a document has been durably indexed. Datashare records
 * completed keys and hands the parser a {@code ResumePolicy} built from that set on a resumed run.
 *
 * <p>Off by default: with no policy in the context (or {@link #NONE}), nothing is skipped and a
 * non-resumed run is byte-for-byte unchanged.
 */
@FunctionalInterface
public interface ResumePolicy {

    /** A policy that resumes nothing: every unit is treated as not-yet-done. The default. */
    ResumePolicy NONE = resumeKey -> false;

    /**
     * @param resumeKey the stable, cross-run resumable key of a work unit. For PST/OST this is a
     *                  mail message's descriptor node id ({@code PSTMessage.getDescriptorNodeId()}),
     *                  a property of the mailbox's bytes and thus identical every run.
     * @return {@code true} if this unit was already emitted and durably indexed by a previous run,
     *         so the current run may skip re-parsing and re-emitting it. MUST be a pure function of
     *         the key (side-effect free) and stable for the lifetime of one extraction.
     */
    boolean isUnitDone(long resumeKey);
}
