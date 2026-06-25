package org.icij.extract.parser;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.fest.assertions.Assertions.assertThat;

public class PstStdoutFilterTest {

    @Test
    public void suppressible_dropsWritesWhileFlagged_passesThroughOtherwise() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        boolean[] suppress = {false};
        PrintStream stream = PstStdoutFilter.suppressible(sink, () -> suppress[0]);

        stream.println("keep me");
        suppress[0] = true;
        stream.println("Unknown message type: IPM.AbchPerson"); // java-libpst noise
        suppress[0] = false;
        stream.println("keep again");
        stream.flush();

        String out = sink.toString();
        assertThat(out).contains("keep me");
        assertThat(out).contains("keep again");
        assertThat(out).excludes("Unknown message type");
    }

    @Test
    public void beginEnd_toggleTheCurrentThreadFlag() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        PrintStream stream = PstStdoutFilter.suppressible(sink, PstStdoutFilter::isSuppressingCurrentThread);

        stream.println("before begin");
        PstStdoutFilter.begin();
        try {
            stream.println("during parse");
        } finally {
            PstStdoutFilter.end();
        }
        stream.println("after end");
        stream.flush();

        String out = sink.toString();
        assertThat(out).contains("before begin");
        assertThat(out).contains("after end");
        assertThat(out).excludes("during parse");
    }
}
