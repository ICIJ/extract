package org.icij.extract.parser;

import com.pff.PSTFile;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Proves the PER-TASK PSTFile handle ownership contract (F2): a folder task closes its OWN handle in
 * its OWN finally, on its OWN thread, whether the detect+emit body returns normally or throws. This is
 * what makes the close-during-read race impossible by construction: no controller ever closes a handle
 * a task is still using.
 *
 * <p>The prior version of this test drove {@code awaitQuietlyForTest} on a future that was never
 * cancelled, so it never exercised the abort path and proved nothing; that seam is now deleted.
 */
public class ParallelWalkCleanupTest {

    private static final String FIXTURE = "/documents/pst/testPST.pst";

    // A real PSTFile handle whose getFileHandle() is a RandomAccessFile that counts its own close()
    // calls, so the test can assert the task's finally closed the handle exactly once.
    private static final class CloseCountingPSTFile extends PSTFile {
        private final CloseCountingRandomAccessFile spyHandle;

        CloseCountingPSTFile(final String path) throws Exception {
            super(path);
            this.spyHandle = new CloseCountingRandomAccessFile(super.getFileHandle());
        }

        @Override
        public RandomAccessFile getFileHandle() {
            return spyHandle;
        }

        int closeCount() {
            return spyHandle.closeCount.get();
        }
    }

    // Delegates reads to the real handle but records every close(). Constructed against the same file
    // the real handle opened, so its own descriptor is a harmless spare that closing releases.
    private static final class CloseCountingRandomAccessFile extends RandomAccessFile {
        final AtomicInteger closeCount = new AtomicInteger();

        CloseCountingRandomAccessFile(final RandomAccessFile real) throws Exception {
            super(pathOf(real), "r");
        }

        private static String pathOf(final RandomAccessFile real) throws Exception {
            // The spy only needs to be a valid, closeable RandomAccessFile; reuse the fixture path.
            return ParallelWalkCleanupTest.class.getResource(FIXTURE).getPath();
        }

        @Override
        public void close() throws java.io.IOException {
            closeCount.incrementAndGet();
            super.close();
        }
    }

    @Test(timeout = 30_000)
    public void testTaskClosesOwnHandleOnThrow() throws Exception {
        final ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        final CloseCountingPSTFile handle =
                new CloseCountingPSTFile(getClass().getResource(FIXTURE).getPath());

        // Descriptor id that resolves to no loadable object, so walkOneFolder's detectAndLoadPSTObject
        // throws -> the task body aborts -> the finally must still close the handle. baseEmission is null
        // because emitFolderParallel is only reached for a resolved PSTFolder, which never happens here.
        parser.runOwnedFolderTask(handle, Integer.MAX_VALUE, "/bogus", null, null);

        assertThat(handle.closeCount()).isEqualTo(1);
    }

    @Test(timeout = 30_000)
    public void testTaskClosesOwnHandleOnNormalCompletion() throws Exception {
        // A descriptor that is not a PSTFolder returns cleanly (no emit); the finally must still close.
        final ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        final CloseCountingPSTFile handle =
                new CloseCountingPSTFile(getClass().getResource(FIXTURE).getPath());

        // Descriptor 0 is not a normal loadable folder object; walkOneFolder handles it without emitting.
        parser.runOwnedFolderTask(handle, 0, "/", null, null);

        assertThat(handle.closeCount()).isEqualTo(1);
    }
}
