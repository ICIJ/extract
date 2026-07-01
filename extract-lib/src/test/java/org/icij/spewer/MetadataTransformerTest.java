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
import java.time.Instant;
import java.util.List;
import java.util.Optional;

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

    @Test
    public void test_toIso8601Array_returns_normalized_list_when_all_values_are_iso() {
        String[] values = {"2023-05-30T12:35:06Z", "2024-06-20T06:39:27Z"};
        assertThat(MetadataTransformer.toIso8601Array(values))
                .isEqualTo(Optional.of(List.of("2023-05-30T12:35:06Z", "2024-06-20T06:39:27Z")));
    }

    @Test
    public void test_toIso8601Array_normalizes_bare_dates_to_midnight_utc() {
        String[] values = {"2015-06-03", "2016-01-01"};
        assertThat(MetadataTransformer.toIso8601Array(values))
                .isEqualTo(Optional.of(List.of("2015-06-03T00:00:00Z", "2016-01-01T00:00:00Z")));
    }

    @Test
    public void test_toIso8601Array_is_empty_for_lenient_non_iso_formats() {
        String[] values = {"Tue Jan 27 17:03:21 2004", "2023-05-30T12:35:06Z"};
        assertThat(MetadataTransformer.toIso8601Array(values)).isEqualTo(Optional.empty());
    }

    @Test
    public void test_toIso8601Array_is_empty_when_any_value_is_not_a_date() {
        String[] values = {"2023-05-30T12:35:06Z", "not a date"};
        assertThat(MetadataTransformer.toIso8601Array(values)).isEqualTo(Optional.empty());
    }

    @Test
    public void test_toIso8601Array_is_empty_when_no_value_is_a_date() {
        String[] values = {"alpha", "beta"};
        assertThat(MetadataTransformer.toIso8601Array(values)).isEqualTo(Optional.empty());
    }

    @Test
    public void test_parseInstant_parses_offset_datetime() {
        assertThat(MetadataTransformer.parseInstant("2023-05-30T13:35:06+01:00"))
                .isEqualTo(Optional.of(Instant.parse("2023-05-30T12:35:06Z")));
    }
}