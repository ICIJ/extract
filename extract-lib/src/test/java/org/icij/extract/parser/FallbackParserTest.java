package org.icij.extract.parser;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.extractor.NoContentReason;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.fest.assertions.Assertions.assertThat;

public class FallbackParserTest {

    @Test
    public void testNonEmptyUnsupportedIsMarkedAndDoesNotThrow() throws Exception {
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_LENGTH, "3");

        FallbackParser.INSTANCE.parse(
                new ByteArrayInputStream(new byte[]{1, 2, 3}),
                new BodyContentHandler(), metadata, new ParseContext());

        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("unsupported-media-type");
    }

    @Test
    public void testEmptyIsMarkedEmptyFile() throws Exception {
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_LENGTH, "0");

        FallbackParser.INSTANCE.parse(
                new ByteArrayInputStream(new byte[0]),
                new BodyContentHandler(), metadata, new ParseContext());

        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("empty-file");
    }
}
