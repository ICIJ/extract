package org.icij.extract.parser;

import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.fest.assertions.Assertions.assertThat;

public class ResourceClosingReaderTest {

    @Test
    public void testClosesExtraResourceOnClose() throws Exception {
        AtomicBoolean resourceClosed = new AtomicBoolean(false);
        Reader reader = new ResourceClosingReader(new StringReader("hi"), () -> resourceClosed.set(true));

        assertThat(resourceClosed.get()).isFalse();
        reader.close();
        assertThat(resourceClosed.get()).isTrue();
    }

    @Test
    public void testStillReadsThroughDelegate() throws Exception {
        try (Reader reader = new ResourceClosingReader(new StringReader("hi"), () -> {})) {
            assertThat((char) reader.read()).isEqualTo('h');
            assertThat((char) reader.read()).isEqualTo('i');
            assertThat(reader.read()).isEqualTo(-1);
        }
    }
}
