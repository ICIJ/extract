package org.icij.extract.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Per-attachment fallback reader backed by libpff (via its Python binding, {@code pypff}).
 *
 * <p>java-libpst 0.9.3 mis-reads by-value attachments that span multiple blocks in OST 2013
 * ("64-bit, 4k page") files: it either throws {@code IndexOutOfBoundsException} from
 * {@code PSTNodeInputStream}, or returns corrupt/truncated bytes, or fails to inflate a
 * compressed block. libpff handles that format correctly. This class recovers exactly the
 * attachment the JVM side failed on — selected by the message descriptor node id (which equals
 * {@code PSTMessage.getDescriptorNodeId()} and {@code pypff} {@code message.identifier}) plus the
 * attachment index — and hands its bytes back as a temp file for re-emission through Tika.
 *
 * <p>It is a <em>fallback</em>: it is invoked only for attachments {@link ResilientOutlookPSTParser}
 * already detected as unreadable, so the healthy fast path is untouched. It is also best-effort and
 * never throws: when libpff / pypff / python3 are unavailable, or recovery fails for any reason, it
 * returns {@link Optional#empty()} and the caller falls back to today's behaviour (count the loss).
 */
final class LibpffAttachmentRecovery {

    private static final Logger logger = LoggerFactory.getLogger(LibpffAttachmentRecovery.class);

    private static final String SCRIPT_RESOURCE = "/org/icij/extract/parser/pff_read_attachment.py";
    // A single attachment recovery should be quick; bound it so a wedged child can never hang the parse.
    private static final long PROCESS_TIMEOUT_SECONDS = 120;

    // Resolved once: the python interpreter, the extracted script path, and whether the toolchain
    // (python3 + pypff) is usable at all. null script => unavailable.
    private static volatile Boolean available;
    private static volatile Path scriptPath;

    private LibpffAttachmentRecovery() {}

    /**
     * @return {@code true} when python3 + pypff are importable and the bridge script is extractable,
     *         so recovery can be attempted; {@code false} otherwise (caller stays on the count-only path).
     */
    static synchronized boolean isAvailable() {
        if (available != null) {
            return available;
        }
        available = probe();
        return available;
    }

    private static boolean probe() {
        try {
            scriptPath = extractScript();
        } catch (final IOException e) {
            logger.info("libpff attachment recovery disabled: cannot stage bridge script ({}).", e.toString());
            return false;
        }
        try {
            final Process p = new ProcessBuilder("python3", "-c", "import pypff")
                    .redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            if (!p.waitFor(15, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return false;
            }
            final boolean ok = p.exitValue() == 0;
            logger.info("libpff attachment recovery {}.", ok ? "available (python3 + pypff present)"
                    : "disabled (python3 present but pypff import failed)");
            return ok;
        } catch (final IOException e) {
            logger.info("libpff attachment recovery disabled: python3 not runnable ({}).", e.toString());
            return false;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static Path extractScript() throws IOException {
        final Path tmp = Files.createTempFile("pff_read_attachment", ".py");
        tmp.toFile().deleteOnExit();
        try (final InputStream in = LibpffAttachmentRecovery.class.getResourceAsStream(SCRIPT_RESOURCE)) {
            if (in == null) {
                throw new IOException("bridge script resource missing: " + SCRIPT_RESOURCE);
            }
            Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmp;
    }

    /**
     * Recover one attachment's bytes via libpff into a temp file. Best-effort: returns empty on any
     * failure (toolchain missing, child error/timeout, or a byte count that disagrees with the
     * attachment's declared size). The caller owns the returned file and must delete it.
     *
     * @param pstPath          path to the OST/PST being parsed
     * @param messageId        message descriptor node id ({@code getDescriptorNodeId()} == pypff identifier)
     * @param attachmentIndex  zero-based attachment index within that message
     * @param declaredSize     attachment's declared size, used to validate the recovery; ignored if <= 0
     * @return the temp file holding the recovered bytes, or empty when recovery was not possible
     */
    static Optional<Path> recoverToTempFile(final String pstPath, final long messageId,
                                            final int attachmentIndex, final long declaredSize) {
        if (!isAvailable()) {
            return Optional.empty();
        }
        Path out = null;
        try {
            out = Files.createTempFile("pff_recovered_attachment", ".bin");
            final Process p = new ProcessBuilder("python3", scriptPath.toString(), pstPath,
                    Long.toString(messageId), Integer.toString(attachmentIndex))
                    .redirectOutput(out.toFile())
                    .redirectError(ProcessBuilder.Redirect.PIPE)
                    .start();
            final byte[] err = p.getErrorStream().readAllBytes();
            if (!p.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                logger.warn("libpff recovery timed out for message {} attachment {} in \"{}\".",
                        messageId, attachmentIndex, pstPath);
                return discard(out);
            }
            if (p.exitValue() != 0) {
                logger.warn("libpff recovery failed (exit {}) for message {} attachment {} in \"{}\": {}",
                        p.exitValue(), messageId, attachmentIndex, pstPath, new String(err).trim());
                return discard(out);
            }
            final long recovered = Files.size(out);
            if (recovered <= 0 || (declaredSize > 0 && recovered != declaredSize)) {
                logger.warn("libpff recovery size mismatch for message {} attachment {} in \"{}\": "
                        + "got {} bytes, expected {}.", messageId, attachmentIndex, pstPath, recovered, declaredSize);
                return discard(out);
            }
            return Optional.of(out);
        } catch (final IOException e) {
            logger.warn("libpff recovery errored for message {} attachment {} in \"{}\".",
                    messageId, attachmentIndex, pstPath, e);
            return discard(out);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return discard(out);
        }
    }

    private static Optional<Path> discard(final Path out) {
        if (out != null) {
            try {
                Files.deleteIfExists(out);
            } catch (final IOException ignored) {
                // temp file cleanup is best-effort
            }
        }
        return Optional.empty();
    }
}
