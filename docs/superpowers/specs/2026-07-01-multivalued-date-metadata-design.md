# Preserve multi-valued date metadata instead of comma-joining it

**Date:** 2026-07-01
**Module:** extract-lib
**Status:** approved, ready for implementation plan

## Problem

Documents are silently dropped from the Elasticsearch index when a metadata date field holds multiple values.

`Spewer.MapValuesConsumer.accept` (`extract-lib/src/main/java/org/icij/spewer/Spewer.java:197`) collapses every multi-valued field with `String.join(",", values)`. When Elasticsearch has date-detected that field, the joined string is not a valid date and the ES date mapper throws `mapper_parsing_exception`, which fails the whole document, not just the field.

Concrete case: a signed PDF with two signatures produces two `tika_metadata_signature_date` values. Each value on its own is clean ISO-8601 (e.g. `2023-05-30T12:35:06Z`), so ES date-detects the field from single-signature documents. A two-signature document then sends `"2023-05-30T12:35:06Z,2024-06-20T06:39:27Z"`, which fails to parse and drops the document (data loss).

### What is already done (complementary, separate)

The datashare ES mapping was hardened with a `dynamic_template` (`metadata_dates`, `path_match: metadata.*`, `match_mapping_type: date`) giving date-detected `metadata.*` fields `ignore_malformed: true`. That only *tolerates* the bad value (drops the field, keeps the document). This Extract fix is what actually *preserves* the dates: emit multi-valued dates well-formed so they index correctly.

## Goal

In the multi-valued path, when every value of a field parses as a date, ISO-normalize each value and emit them as a JSON array (ES date fields accept arrays of dates) instead of comma-joining. Keep non-date multi-valued fields comma-joined exactly as today.

## Key decisions

1. **Detection is parse-based, not `dateProperties`-based.** The reported failing field, `signature_date`, is not in the hardcoded `dateProperties` map, so keying off that map would not fix the bug. ES types fields by detection (`date_detection` / the `metadata_dates` dynamic template), so the fix matches that behavior: "all values parse as dates -> emit an ISO array." Blast radius is exactly the set of fields ES would date-type, i.e. the ones already at risk.

2. **Raw field holds an array of ISO-normalized values.** Each detected date value is normalized to an ISO-8601 instant via `Instant.toString()`. Normalizing (rather than emitting original strings) guarantees every array element is ES-parseable, so messier formats are preserved rather than dropped by `ignore_malformed`.

3. **No `_iso8601` companion for multi-valued fields.** In the multi-valued case the raw field is already an ISO array, so a companion would be redundant. No multi-valued companion exists today, so nothing downstream depends on it.

4. **Change is confined to `MapValuesConsumer`.** Of the five `ValueArrayConsumer` implementations, only `MapValuesConsumer` (the ES/DB path via `DatabaseSpewer`) joins in a way that breaks ES. `FileSpewer` and `RESTSpewer` already emit arrays / repeated values; `PrintStreamSpewer` joins for human display. The fix therefore needs no interface change and touches no other spewer. The date-parsing logic lives as a reusable static helper on `MetadataTransformer`, where `dateFormats` already lives.

## Design

### 1. New static helpers on `MetadataTransformer`

Reuse the existing `dateFormats` list. Add:

- `static Optional<Instant> parseInstant(String value)` — parse only the ISO-8601 forms Elasticsearch's `date_detection` actually recognizes (`strict_date_optional_time`), in order: `Instant.parse`, then `OffsetDateTime.parse`, then `LocalDateTime.parse` (defaulted to UTC), then `LocalDate.parse` (bare date -> midnight UTC, which is exactly how ES parses a date-only value). Returns empty if none match. It deliberately does NOT use the lenient `dateFormats` list.

- `static Optional<List<String>> toIso8601Array(String[] values)` — map every value through `parseInstant`; return the list of `instant.toString()` **iff every value parses**, otherwise `Optional.empty()`. This is detection and normalization in one call.

Note: detection is scoped to exactly the formats ES date-detects, so the "blast radius = the set of fields ES would date-type" guarantee holds precisely. Two consequences of this scoping, both intended:
- Lenient formats (RFC-1123, `EEE MMM d HH:mm:ss uuuu`) are NOT detected. ES maps such fields as `text`/`keyword`, never `date`, so comma-joining them is safe and their `_source`/aggregation shape is left unchanged.
- Bare dates such as `2015-06-03` ARE detected (via `LocalDate`), because `strict_date_optional_time` date-types them; a multi-valued bare-date field would otherwise be comma-joined and dropped by the ES date mapper.

### 2. `Spewer.MapValuesConsumer.accept`

```java
public void accept(String name, String[] values) {
    MetadataTransformer.toIso8601Array(values)
        .ifPresentOrElse(
            iso -> map.put(name, iso),                     // List -> JSON array of ISO instants
            () -> map.put(name, String.join(",", values))  // unchanged for non-dates
        );
}
```

The metadata map is `Map<String, Object>`; a `List` value serializes to a JSON array through Jackson, which is what feeds ES via `DatabaseSpewer`. Only fields where **every** value parses as a date change shape; everything else stays byte-identical.

## Explicitly unchanged (no regression)

- **Single-valued path and `transformDate`** — untouched. Leaving `transformDate` byte-identical avoids any risk of changing single-valued output. Single-valued `signature_date` already indexes fine; known date-properties keep their raw value plus their `_iso8601` companion exactly as today. `parseInstant` shares no code with `transformDate` and does not use `dateFormats`, so there is no duplication between them.
- **`FileSpewer` / `RESTSpewer` / `PrintStreamSpewer`** — untouched.
- **Non-date multi-valued fields** — still comma-joined.
- **`ValueArrayConsumer` interface** — unchanged.

## Elasticsearch compatibility

Elasticsearch has no "array type": every field is intrinsically multi-valued, so a field mapped as `date` accepts one date or an array of dates under the same mapping. No reindex is needed and previously indexed scalar values coexist with new array values.

Both mapping histories are safe:

| First value ES saw for the field | Resulting mapping | Our ISO array -> |
| --- | --- | --- |
| Single clean ISO date | `date` | date array indexes fine (this is the case dropping documents today) |
| Multi-value pre-fix comma string | `text` / `keyword` (a comma string does not date-detect) | array of keyword strings indexes fine |

The scalar-vs-array distinction survives only in `_source`, never in the index; sorting and aggregations on a now-multi-valued date field work normally (ES uses a configurable min/max mode for multi-valued sort). Because detection requires **all** values to parse before we emit an array, we never send a partially-invalid date array; a mixed field falls back to comma-join, with the datashare `ignore_malformed` template as the backstop.

## Edge cases

- `MapValuesConsumer` only ever receives arrays of length > 1 (single values route to `MapValueConsumer`), and `MetadataTransformer.transform` has already stripped empty values before calling the consumer.
- Mixed field (at least one non-date value) -> not detected -> comma-joined -> `ignore_malformed` backstop still applies.
- Input order is preserved. Date fields are not in `deduplicateProperties`, so duplicate dates are kept as-is; ES handles duplicate values in a date array.

## Tests (JUnit4 / Maven, extract-lib)

- **`MetadataTransformerTest`** — unit-test `toIso8601Array`:
  - all-ISO-instant values -> `Optional` of the normalized ISO list (the two-signature scenario);
  - bare `LocalDate` values (`2015-06-03`) -> normalized to midnight-UTC instants;
  - a lenient-format value (`Tue Jan 27 17:03:21 2004`) -> `Optional.empty()` (NOT detected, so the field falls back to comma-join);
  - mixed (one non-date) -> `Optional.empty()`;
  - all non-date -> `Optional.empty()`.
- **`SpewerTest`** — exercise the real `Spewer.getMetadata` (protected, via a stub subclass that calls it) and assert:
  - a multi-valued date field -> a `List` of ISO instants in the returned map;
  - a multi-valued non-date field -> a comma-joined `String`;
  - a single-valued date field is unaffected (existing `testWritesISO8601Dates` continues to pass).
