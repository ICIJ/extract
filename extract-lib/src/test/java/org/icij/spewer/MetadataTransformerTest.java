package org.icij.spewer;


import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Test
    public void test_parseFallbackDate_parses_double_space_padded_ctime() {
        // C asctime pads a single-digit day to width 2, producing two spaces before the day.
        assertThat(MetadataTransformer.parseFallbackDate("Thu May  1 17:27:09 1997"))
                .isEqualTo(Optional.of(Instant.parse("1997-05-01T17:27:09Z")));
    }

    @Test
    public void test_parseFallbackDate_parses_single_space_ctime() {
        assertThat(MetadataTransformer.parseFallbackDate("Tue Jan 27 17:03:21 2004"))
                .isEqualTo(Optional.of(Instant.parse("2004-01-27T17:03:21Z")));
    }

    @Test
    public void test_parseFallbackDate_parses_bare_epoch_seconds() {
        assertThat(MetadataTransformer.parseFallbackDate("1705133865"))
                .isEqualTo(Optional.of(Instant.parse("2024-01-13T08:17:45Z")));
    }

    @Test
    public void test_parseFallbackDate_parses_bare_epoch_milliseconds() {
        assertThat(MetadataTransformer.parseFallbackDate("1705133865000"))
                .isEqualTo(Optional.of(Instant.parse("2024-01-13T08:17:45Z")));
    }

    @Test
    public void test_parseFallbackDate_parses_compact_date_time_with_minutes() {
        // "yyyyMMddHHmm" (12 digits): must parse as a 2023 date, NOT be misread as epoch seconds (year ~8377).
        assertThat(MetadataTransformer.parseFallbackDate("202312312359"))
                .isEqualTo(Optional.of(Instant.parse("2023-12-31T23:59:00Z")));
    }

    @Test
    public void test_parseFallbackDate_parses_compact_date_time_with_seconds() {
        // "yyyyMMddHHmmss" (14 digits): must parse as a 2023 date, NOT be misread as epoch millis (year ~2611).
        assertThat(MetadataTransformer.parseFallbackDate("20231231235959"))
                .isEqualTo(Optional.of(Instant.parse("2023-12-31T23:59:59Z")));
    }

    @Test
    public void test_parseFallbackDate_parses_compact_date() {
        assertThat(MetadataTransformer.parseFallbackDate("20231231"))
                .isEqualTo(Optional.of(Instant.parse("2023-12-31T00:00:00Z")));
    }

    @Test
    public void test_parseFallbackDate_is_empty_for_implausible_epoch() {
        // A genuinely huge all-digit value maps to an absurd year, so it degrades to the raw string.
        assertThat(MetadataTransformer.parseFallbackDate("999999999999999"))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void test_parseFallbackDate_is_empty_for_corrupt_date() {
        assertThat(MetadataTransformer.parseFallbackDate("Thu May  0:00:00 17:37:52 1997"))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void test_transform_keeps_raw_value_and_skips_iso_when_date_is_unparseable() throws Exception {
        // A cosmetic, unparseable date must never veto the whole document: the raw value is kept,
        // only the ISO-normalized variant is skipped, and no exception is thrown.
        Metadata metadata = new Metadata();
        final String field = DublinCore.CREATED.getName();
        metadata.add(field, "Thu May  0:00:00 17:37:52 1997");

        final FieldNames fields = new FieldNames();
        final Map<String, String> single = new HashMap<>();
        new MetadataTransformer(metadata, fields).transform(single::put, (name, values) -> {});

        assertThat(single.get(fields.forMetadata(field))).isEqualTo("Thu May  0:00:00 17:37:52 1997");
        assertThat(single.containsKey(fields.forMetadataISODate(field))).isFalse();
    }

    @Test
    public void test_transform_emits_iso_for_double_space_ctime_date() throws Exception {
        Metadata metadata = new Metadata();
        final String field = DublinCore.CREATED.getName();
        metadata.add(field, "Thu May  1 17:27:09 1997");

        final FieldNames fields = new FieldNames();
        final Map<String, String> single = new HashMap<>();
        new MetadataTransformer(metadata, fields).transform(single::put, (name, values) -> {});

        assertThat(single.get(fields.forMetadata(field))).isEqualTo("Thu May  1 17:27:09 1997");
        assertThat(single.get(fields.forMetadataISODate(field))).isEqualTo("1997-05-01T17:27:09Z");
    }
}