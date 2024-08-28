package org.icij.spewer;


import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.tika.metadata.Metadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.fest.assertions.Assertions.assertThat;

public class MetadataTransformerTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void test_transform_metadata() throws Exception{
        Metadata metadata = new Metadata();
        metadata.add("foo", "bar");
        metadata.add("baz", "qux");
        assertThat(new MetadataTransformer(metadata).transform()).isEqualTo("{\"foo\":[\"bar\"],\"baz\":[\"qux\"]}");
    }

    @Test
    public void test_transform_metadata_with_case() throws Exception {
        Metadata metadata = new Metadata();
        metadata.add("fooBar", "baz");
        assertThat(new MetadataTransformer(metadata).transform()).isEqualTo("{\"fooBar\":[\"baz\"]}");
    }

    @Test
    public void test_transform_metadata_with_value_consumers() throws Exception{
        Metadata metadata = new Metadata();
        metadata.add("foo", "bar");
        metadata.add("baz", "qux");
        try (final JsonGenerator jsonGenerator = new JsonFactory().createGenerator(tmp.newFile("metadata.json"), JsonEncoding.UTF8)) {
            jsonGenerator.useDefaultPrettyPrinter();
            jsonGenerator.writeStartObject();

            MetadataTransformer.ValueArrayConsumer valueArrayConsumer = (name, values) -> {
                jsonGenerator.writeArrayFieldStart(name);
                for (String value : values) {
                    jsonGenerator.writeString(value);
                }
                jsonGenerator.writeEndArray();
            };
            new MetadataTransformer(metadata, new FieldNames()).transform(jsonGenerator::writeStringField, valueArrayConsumer);

            jsonGenerator.writeEndObject();
            jsonGenerator.writeRaw('\n');
        }
        String content = Files.readString(tmp.getRoot().toPath().resolve("metadata.json"));
        assertThat(content).isEqualTo("""
                    {
                      "tika_metadata_baz" : "qux",
                      "tika_metadata_foo" : "bar"
                    }
                    """);
    }

    @Test
    public void test_load_metadata() throws IOException {
        Metadata metadata = new Metadata();
        metadata.add("foo", "bar");
        metadata.add("bar", "qux");
        File metadataFile = tmp.newFile("metadata.json");
        Files.writeString(metadataFile.toPath(), new MetadataTransformer(metadata).transform());

        assertThat(MetadataTransformer.loadMetadata(metadataFile)).isEqualTo(metadata);
    }
}