package org.icij.extract.parser;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.BooleanSupplier;

/**
 * Suppresses java-libpst's direct {@code System.out}/{@code System.err} output (raw
 * {@code println} that bypasses slf4j, so logback cannot demote it) on threads that are
 * actively parsing a PST. Installed once over the JVM streams; transparent pass-through
 * for every other thread, so the rest of the process is unaffected.
 *
 * <p>Our own diagnostics go through slf4j to a different appender, so the recovery and
 * reconciliation signal survives while the library noise is dropped.
 */
final class PstStdoutFilter {

    private static final ThreadLocal<Boolean> IN_PARSE = ThreadLocal.withInitial(() -> Boolean.FALSE);
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
        IN_PARSE.set(Boolean.TRUE);
    }

    static void end() {
        IN_PARSE.remove();
    }

    static boolean isSuppressingCurrentThread() {
        return IN_PARSE.get();
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
