package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.icij.extract.document.TikaDocument.ReaderGenerator;
import org.icij.spewer.Spewer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.fest.assertions.Assertions.assertThat;

public class BudgetedEmbedBufferTest {

    private TemporaryResources tmp;

    @Before public void setUp() { tmp = new TemporaryResources(); }
    @After public void tearDown() throws Exception { tmp.close(); }

    private static String read(ReaderGenerator generator) throws Exception {
        try (Reader reader = generator.generate()) {
            return Spewer.toString(reader);
        }
    }

    @Test
    public void testStaysInMemoryUnderBudget() throws Exception {
        AtomicLong reserved = new AtomicLong();
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, 1024, tmp);

        buffer.write("hello".getBytes(StandardCharsets.UTF_8));
        buffer.close();

        assertThat(buffer.isSpilled()).isFalse();
        assertThat(reserved.get()).isEqualTo(5L);
        assertThat(read(buffer.readerGenerator())).isEqualTo("hello");
    }

    @Test
    public void testSpillsWhenSingleWriteExceedsBudget() throws Exception {
        AtomicLong reserved = new AtomicLong();
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, 4, tmp);

        buffer.write("hello world".getBytes(StandardCharsets.UTF_8));
        buffer.close();

        assertThat(buffer.isSpilled()).isTrue();
        assertThat(reserved.get()).isEqualTo(0L); // nothing left resident in memory
        assertThat(read(buffer.readerGenerator())).isEqualTo("hello world");
    }

    @Test
    public void testSharedBudgetForcesLaterBufferToSpill() throws Exception {
        AtomicLong reserved = new AtomicLong();

        BudgetedEmbedBuffer first = new BudgetedEmbedBuffer(reserved, 8, tmp);
        first.write("12345678".getBytes(StandardCharsets.UTF_8)); // exactly fills the budget
        first.close();
        assertThat(first.isSpilled()).isFalse();

        BudgetedEmbedBuffer second = new BudgetedEmbedBuffer(reserved, 8, tmp);
        second.write("9".getBytes(StandardCharsets.UTF_8)); // would exceed -> spills
        second.close();

        assertThat(second.isSpilled()).isTrue();
        assertThat(read(first.readerGenerator())).isEqualTo("12345678");
        assertThat(read(second.readerGenerator())).isEqualTo("9");
    }

    @Test
    public void testSpillsUnderMemoryPressureEvenWhenUnderByteBudget() throws Exception {
        AtomicLong reserved = new AtomicLong();
        // Huge byte budget so only memory pressure can trigger the spill.
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, Long.MAX_VALUE, tmp, () -> true);

        buffer.write("hello".getBytes(StandardCharsets.UTF_8));
        buffer.close();

        assertThat(buffer.isSpilled()).isTrue();
        assertThat(reserved.get()).isEqualTo(0L);
        assertThat(read(buffer.readerGenerator())).isEqualTo("hello");
    }

    @Test
    public void testStaysInMemoryWhenNoPressureAndUnderBudget() throws Exception {
        AtomicLong reserved = new AtomicLong();
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, Long.MAX_VALUE, tmp, () -> false);

        buffer.write("hello".getBytes(StandardCharsets.UTF_8));
        buffer.close();

        assertThat(buffer.isSpilled()).isFalse();
        assertThat(read(buffer.readerGenerator())).isEqualTo("hello");
    }

    @Test
    public void testDiscardAfterSpillReleasesStreamWithoutError() throws Exception {
        AtomicLong reserved = new AtomicLong();
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, 0, tmp); // 0 budget => spills immediately

        buffer.write("spilled".getBytes(StandardCharsets.UTF_8));
        assertThat(buffer.isSpilled()).isTrue();

        buffer.discard();       // must release the open spill stream, not leak it
        buffer.close();         // must remain safe after discard

        assertThat(reserved.get()).isEqualTo(0L);
    }

    @Test
    public void testDiscardReleasesReservedBytes() throws Exception {
        AtomicLong reserved = new AtomicLong();
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, 1024, tmp);

        buffer.write("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(reserved.get()).isEqualTo(5L);

        buffer.discard();
        assertThat(reserved.get()).isEqualTo(0L);
    }

    @Test
    public void testWriteAfterCloseThrows() throws Exception {
        AtomicLong reserved = new AtomicLong();
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, 1024, tmp);
        buffer.close();
        try {
            buffer.write("x".getBytes(StandardCharsets.UTF_8));
            org.junit.Assert.fail("expected IOException");
        } catch (IOException expected) {
            // expected
        }
    }

    @Test
    public void testReaderGeneratorThrowsAfterDiscard() throws Exception {
        AtomicLong reserved = new AtomicLong();
        BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, 1024, tmp);
        buffer.write("hello".getBytes(StandardCharsets.UTF_8));
        buffer.discard();
        org.icij.extract.document.TikaDocument.ReaderGenerator generator = buffer.readerGenerator();
        try {
            generator.generate();
            org.junit.Assert.fail("expected IOException");
        } catch (IOException expected) {
            // expected
        }
    }

    /**
     * Concurrency regression test: N threads each own one buffer but share a single AtomicLong
     * reserved and a small budgetBytes. All threads fire their write() simultaneously via a
     * CountDownLatch. After all writes settle the shared counter must not exceed budgetBytes
     * (i.e. the atomic reserve-then-check prevented any budget overrun), and every buffer
     * whose reservation was rolled back must have spilled to disk.
     */
    @Test
    public void testConcurrentWritesRespectSharedBudget() throws Exception {
        final int threads = 8;
        final int chunkSize = 512;          // each buffer writes 512 bytes
        final long budgetBytes = 1024;      // fits at most 2 chunks in memory
        final AtomicLong reserved = new AtomicLong();

        final byte[] chunk = new byte[chunkSize];
        java.util.Arrays.fill(chunk, (byte) 'x');

        final CountDownLatch startGate = new CountDownLatch(1);
        final List<BudgetedEmbedBuffer> buffers = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            buffers.add(new BudgetedEmbedBuffer(reserved, budgetBytes, tmp));
        }

        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final List<Future<?>> futures = new ArrayList<>(threads);
        for (final BudgetedEmbedBuffer buf : buffers) {
            futures.add(pool.submit(() -> {
                try {
                    startGate.await();
                    buf.write(chunk, 0, chunkSize);
                    buf.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }

        startGate.countDown(); // release all threads at once to maximise interleaving
        for (final Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        // The shared in-memory counter must never exceed the budget.
        assertThat(reserved.get()).isLessThanOrEqualTo(budgetBytes);

        // Every buffer that kept its bytes in memory must have contributed to the counter;
        // every buffer that spilled must have released its reservation (reserved == 0 for it).
        long expectedReserved = 0;
        int spilledCount = 0;
        for (final BudgetedEmbedBuffer buf : buffers) {
            if (!buf.isSpilled()) {
                expectedReserved += chunkSize;
            } else {
                spilledCount++;
            }
        }
        assertThat(reserved.get()).isEqualTo(expectedReserved);
        // With budget=1024 and chunkSize=512, at most 2 buffers can stay in memory.
        assertThat(spilledCount).isGreaterThanOrEqualTo(threads - (int) (budgetBytes / chunkSize));
    }
}
