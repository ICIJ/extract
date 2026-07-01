package org.icij.extract.parser;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.BooleanSupplier;

/**
 * Suppresses all {@code System.out}/{@code System.err} writes on threads that are actively
 * parsing a PST. The primary target is java-libpst's raw {@code println} noise (~1,100+ lines
 * per big-OST parse) that bypasses slf4j entirely; however, because logback's
 * {@code ConsoleAppender} resolves {@code System.out} lazily on every write, the suppression
 * window also gates any slf4j output routed through a console appender — including the parser's
 * own per-item recovery INFO lines and any embedded PDFBox/OCR slf4j output — on the console
 * during the suppressed region. Everything is still preserved in the FILE appender and in the
 * ES-indexed {@code pst_*} metadata produced by the F3 hand-off.
 *
 * <p>Suppression is reference-counted per thread: a {@code .pst}/{@code .ost} attached inside
 * another PST re-enters {@code parse()} on the same thread, so a plain boolean flag would be
 * cleared by the inner parse's {@code end()} and unsuppress the outer parse's remaining ~1,100+
 * noise lines. The depth counter instead keeps the outer parse suppressed until the OUTERMOST
 * parse exits.
 *
 * <p>To keep the load-bearing per-PST reconciliation summary visible on the console,
 * {@code ResilientOutlookPSTParser} emits it via {@link #runWithSuppressionLifted(Runnable)},
 * which temporarily lifts suppression on this thread without disturbing the nesting count, rather
 * than ending suppression early. Per-item recovery INFO lines (high-volume: ~11k on a large OST)
 * remain suppressed on the console — they are noise at scale. Installed exactly once over the JVM
 * streams; transparent pass-through for every other thread.
 */
final class PstStdoutFilter {

    private static final ThreadLocal<Integer> PARSE_DEPTH = ThreadLocal.withInitial(() -> 0);
    // While true on the current thread, suppression is temporarily lifted even mid-parse, so the
    // per-PST reconciliation summary reaches the console. Separate from depth so it never disturbs
    // the nesting count.
    private static final ThreadLocal<Boolean> SUPPRESSION_LIFTED = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private static boolean installed = false;

    private PstStdoutFilter() {
    }

    // Wraps System.out/System.err exactly once. Idempotent and synchronized: repeated parses
    // and multiple parser instances never stack wrappers.
    static synchronized void install() {
        if (installed) {
            return;
        }
        System.setOut(suppressible(System.out, PstStdoutFilter::isSuppressingCurrentThread));
        System.setErr(suppressible(System.err, PstStdoutFilter::isSuppressingCurrentThread));
        installed = true;
    }

    static void begin() {
        PARSE_DEPTH.set(PARSE_DEPTH.get() + 1);
    }

    static void end() {
        final int depth = PARSE_DEPTH.get() - 1;
        if (depth <= 0) {
            PARSE_DEPTH.remove();
        } else {
            PARSE_DEPTH.set(depth);
        }
    }

    static boolean isSuppressingCurrentThread() {
        return PARSE_DEPTH.get() > 0 && !SUPPRESSION_LIFTED.get();
    }

    // Runs the given task with suppression armed on the current thread, balanced via try/finally
    // so depth always returns to its prior value even if the task throws. The canonical lifecycle
    // wrapper for fan-out pool threads.
    static void runWithSuppression(final Runnable r) {
        begin();
        try {
            r.run();
        } finally {
            end();
        }
    }

    // Test accessor: the current thread's suppression nesting depth (0 = not suppressing).
    static int depthForTest() {
        return PARSE_DEPTH.get();
    }

    // Runs the given action with console suppression temporarily lifted on this thread, restoring the
    // prior state afterward. Used to emit the per-PST reconciliation summary on the console even though
    // the parse thread is still inside the suppressed region. Nesting-safe (saves/restores prior state).
    static void runWithSuppressionLifted(final Runnable action) {
        final boolean previouslyLifted = SUPPRESSION_LIFTED.get();
        SUPPRESSION_LIFTED.set(Boolean.TRUE);
        try {
            action.run();
        } finally {
            if (previouslyLifted) {
                SUPPRESSION_LIFTED.set(Boolean.TRUE);
            } else {
                SUPPRESSION_LIFTED.remove();
            }
        }
    }

    // A PrintStream whose every byte funnels through a conditional FilterOutputStream: when the
    // supplier says "suppress", the bytes are dropped; otherwise they reach the delegate. Gating
    // at the OutputStream layer (not by overriding PrintStream's text methods) is the reliable
    // interception point, since PrintStream always routes encoded bytes to its underlying stream.
    static PrintStream suppressible(final OutputStream delegate, final BooleanSupplier suppress) {
        final OutputStream gate = new FilterOutputStream(delegate) {
            @Override
            public void write(final int b) throws IOException {
                if (!suppress.getAsBoolean()) {
                    out.write(b);
                }
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                if (!suppress.getAsBoolean()) {
                    out.write(b, off, len);
                }
            }
        };
        return new PrintStream(gate, true);
    }
}
