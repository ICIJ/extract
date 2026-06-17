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
}
