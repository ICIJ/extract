# Multi-Valued Date Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit multi-valued date metadata as a JSON array of ISO-8601 instants instead of a comma-joined string, so Elasticsearch stops dropping documents on `mapper_parsing_exception`.

**Architecture:** Add a parse-based date helper to `MetadataTransformer` (where `dateFormats` already lives). The ES/DB metadata consumer (`Spewer.MapValuesConsumer`) uses it: when every value of a multi-valued field parses as a date, store a `List` of ISO instants (serializes to a JSON array); otherwise keep today's comma-join. No interface change, no edits to other spewers, single-valued path untouched.

**Tech Stack:** Java 17, JUnit 4, FEST assertions, Maven (module `extract-lib`), Apache Tika `Metadata`.

## Global Constraints

- Java 17 (`java.version` in `pom.xml`); use `java.time` APIs already imported in `MetadataTransformer`.
- Do NOT modify the single-valued path or `transformDate` — single-valued output must stay byte-identical.
- Do NOT change the shape of non-date multi-valued fields — they stay comma-joined.
- Do NOT change the `MetadataTransformer.ValueArrayConsumer` interface; only `Spewer.MapValuesConsumer`'s body changes.
- Detection requires ALL values to parse as dates before an array is emitted; a single non-date value falls back to comma-join.
- Commit style: single-line conventional-commit message, no body, no co-author/AI attribution.
- Test command from repo root: `mvn test -pl extract-lib -Dtest=<ClassName>#<methodName>`.

---

### Task 1: Date-detection helper on `MetadataTransformer`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/spewer/MetadataTransformer.java` (add imports; add two static methods after `transformDate`, before the `@FunctionalInterface` block near line 250)
- Test: `extract-lib/src/test/java/org/icij/spewer/MetadataTransformerTest.java`

**Interfaces:**
- Consumes: the existing private static `dateFormats` list in `MetadataTransformer`.
- Produces:
  - `static Optional<Instant> MetadataTransformer.parseInstant(String value)`
  - `static Optional<List<String>> MetadataTransformer.toIso8601Array(String[] values)` — present (list of `Instant.toString()` values, order preserved) iff every value parses; `Optional.empty()` otherwise or if `values` is empty.

- [ ] **Step 1: Write the failing tests**

Add to `MetadataTransformerTest.java`. Add these imports at the top of the file if not present: `import java.time.Instant;`, `import java.util.List;`, `import java.util.Optional;`.

```java
    @Test
    public void test_toIso8601Array_returns_normalized_list_when_all_values_are_iso() {
        String[] values = {"2023-05-30T12:35:06Z", "2024-06-20T06:39:27Z"};
        assertThat(MetadataTransformer.toIso8601Array(values))
                .isEqualTo(Optional.of(List.of("2023-05-30T12:35:06Z", "2024-06-20T06:39:27Z")));
    }

    @Test
    public void test_toIso8601Array_normalizes_lenient_formats() {
        String[] values = {"Tue Jan 27 17:03:21 2004", "2023-05-30T12:35:06Z"};
        assertThat(MetadataTransformer.toIso8601Array(values))
                .isEqualTo(Optional.of(List.of("2004-01-27T17:03:21Z", "2023-05-30T12:35:06Z")));
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn test -pl extract-lib -Dtest=MetadataTransformerTest`
Expected: compilation failure / FAIL — `toIso8601Array` and `parseInstant` do not exist yet.

- [ ] **Step 3: Add imports to `MetadataTransformer.java`**

In the import block (lines 15-32), add:

```java
import java.time.OffsetDateTime;
```
```java
import java.util.Optional;
```

(`Instant`, `LocalDateTime`, `ZoneOffset`, `DateTimeFormatter`, `DateTimeParseException`, `TemporalAccessor`, `ArrayList`, `List` are already imported.)

- [ ] **Step 4: Add the two static helpers**

Insert after the `transformDate` method (which ends at line 249, before the `@FunctionalInterface public interface ValueConsumer` block):

```java
	/**
	 * Parse a single metadata value into an {@link Instant}. Tries strict ISO-8601 forms first
	 * (clean values such as "2023-05-30T12:35:06Z" are not matched by any entry in dateFormats),
	 * then falls back to the lenient formats used by the single-valued path.
	 */
	static Optional<Instant> parseInstant(final String value) {
		if (null == value || value.isEmpty()) {
			return Optional.empty();
		}

		try {
			return Optional.of(Instant.parse(value));
		} catch (final DateTimeParseException ignored) {
			// not a Z-suffixed instant; try the next form
		}

		try {
			return Optional.of(OffsetDateTime.parse(value).toInstant());
		} catch (final DateTimeParseException ignored) {
			// not an offset date-time; try the next form
		}

		try {
			return Optional.of(LocalDateTime.parse(value).toInstant(ZoneOffset.UTC));
		} catch (final DateTimeParseException ignored) {
			// not a zoneless date-time; fall back to the lenient formats
		}

		for (final DateTimeFormatter format : dateFormats) {
			try {
				final TemporalAccessor accessor = format.parseBest(value, Instant::from, LocalDateTime::from);
				if (accessor instanceof Instant) {
					return Optional.of((Instant) accessor);
				} else if (accessor instanceof LocalDateTime) {
					return Optional.of(((LocalDateTime) accessor).toInstant(ZoneOffset.UTC));
				}
			} catch (final DateTimeParseException ignored) {
				// try the next format
			}
		}

		return Optional.empty();
	}

	/**
	 * Normalize a multi-valued field to ISO-8601 instants. Returns the list of normalized values
	 * (order preserved) only if every value parses as a date, so that a mixed field falls back to
	 * the caller's default handling instead of emitting a partially-valid date array.
	 */
	static Optional<List<String>> toIso8601Array(final String[] values) {
		final List<String> normalised = new ArrayList<>(values.length);

		for (final String value : values) {
			final Optional<Instant> instant = parseInstant(value);
			if (instant.isEmpty()) {
				return Optional.empty();
			}
			normalised.add(instant.get().toString());
		}

		return normalised.isEmpty() ? Optional.empty() : Optional.of(normalised);
	}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `mvn test -pl extract-lib -Dtest=MetadataTransformerTest`
Expected: PASS (all tests in the class, including the pre-existing ones).

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/spewer/MetadataTransformer.java extract-lib/src/test/java/org/icij/spewer/MetadataTransformerTest.java
git commit -m "feat(spewer): add toIso8601Array helper for multi-valued dates"
```

---

### Task 2: Emit ISO date arrays from `MapValuesConsumer`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/spewer/Spewer.java:195-198` (`MapValuesConsumer.accept` body)
- Modify: `extract-lib/src/test/java/org/icij/extract/spewer/SpewerTest.java` (expose the real `getMetadata` on `SpewerStub`; add two tests)

**Interfaces:**
- Consumes: `MetadataTransformer.toIso8601Array(String[])` from Task 1; the existing protected `Spewer.getMetadata(TikaDocument)` returning `Map<String, Object>`.
- Produces: metadata map where a detected multi-valued date field holds a `List<String>` of ISO instants and a non-date multi-valued field holds a comma-joined `String`.

- [ ] **Step 1: Write the failing tests**

In `SpewerTest.java`, add these imports if not present: `import java.util.List;` and `import java.util.Map;` (`Map` and `HashMap` are already imported; add `List`).

Add a passthrough to the existing `SpewerStub` inner class (which already `extends Spewer`), so tests can invoke the real consumer path:

```java
		Map<String, Object> realMetadata(final TikaDocument document) throws IOException {
			return getMetadata(document);
		}
```

Add the tests:

```java
	@Test
	public void testMultiValuedDatesBecomeIsoArray() throws IOException {
		final SpewerStub spewer = new SpewerStub();
		final TikaDocument tikaDocument = factory.create("test.txt");
		final Metadata metadata = tikaDocument.getMetadata();
		metadata.add("signature_date", "2023-05-30T12:35:06Z");
		metadata.add("signature_date", "2024-06-20T06:39:27Z");

		final Map<String, Object> result = spewer.realMetadata(tikaDocument);

		assertEquals(List.of("2023-05-30T12:35:06Z", "2024-06-20T06:39:27Z"),
				result.get("tika_metadata_signature_date"));
	}

	@Test
	public void testMultiValuedNonDatesStayCommaJoined() throws IOException {
		final SpewerStub spewer = new SpewerStub();
		final TikaDocument tikaDocument = factory.create("test.txt");
		final Metadata metadata = tikaDocument.getMetadata();
		metadata.add("foo", "alpha");
		metadata.add("foo", "beta");

		final Map<String, Object> result = spewer.realMetadata(tikaDocument);

		assertEquals("alpha,beta", result.get("tika_metadata_foo"));
	}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn test -pl extract-lib -Dtest=SpewerTest`
Expected: `testMultiValuedDatesBecomeIsoArray` FAILS — current `MapValuesConsumer` returns the joined string `"2023-05-30T12:35:06Z,2024-06-20T06:39:27Z"` instead of a `List`. (`testMultiValuedNonDatesStayCommaJoined` should already pass.)

- [ ] **Step 3: Update `MapValuesConsumer.accept`**

In `Spewer.java`, replace the body of `MapValuesConsumer.accept` (lines 195-198):

```java
        @Override
        public void accept(String name, String[] values) throws IOException {
            map.put(name, String.join(",", values));
        }
```

with:

```java
        @Override
        public void accept(String name, String[] values) throws IOException {
            MetadataTransformer.toIso8601Array(values)
                    .ifPresentOrElse(
                            iso -> map.put(name, iso),
                            () -> map.put(name, String.join(",", values)));
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn test -pl extract-lib -Dtest=SpewerTest`
Expected: PASS (including the existing `testWritesISO8601Dates`, confirming the single-valued path is unaffected).

- [ ] **Step 5: Run the full module test suite**

Run: `mvn test -pl extract-lib`
Expected: BUILD SUCCESS — no regression in `FileSpewerTest`, `PrintStreamSpewerTest`, or other spewer tests.

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/spewer/Spewer.java extract-lib/src/test/java/org/icij/extract/spewer/SpewerTest.java
git commit -m "fix(spewer): preserve multi-valued dates as ISO arrays for indexing"
```

---

## Self-Review

**Spec coverage:**
- Parse-based detection → Task 1 (`parseInstant` tries ISO before `dateFormats`; `toIso8601Array` requires all values to parse).
- Raw field holds array of ISO-normalized values → Task 2 (`map.put(name, iso)` where `iso` is `List` of `Instant.toString()`).
- No `_iso8601` companion for multi-valued → no companion code added anywhere.
- Change confined to `MapValuesConsumer`, no interface change → Task 2 modifies only that method body.
- Single-valued path / `transformDate` untouched → not modified; `testWritesISO8601Dates` guards it.
- Non-date multi-valued stays comma-joined → Task 2 `else` branch + `testMultiValuedNonDatesStayCommaJoined`.
- Tests for helper and consumer → Task 1 (`MetadataTransformerTest`) and Task 2 (`SpewerTest`).
- ES compatibility (arrays under a `date` mapping) → verified in spec; no code needed, covered by the array-of-parsed-ISO output.

**Placeholder scan:** none — every step has concrete code and exact commands.

**Type consistency:** `parseInstant` returns `Optional<Instant>`; `toIso8601Array` returns `Optional<List<String>>` and is consumed via `ifPresentOrElse` in Task 2. Field name `tika_metadata_signature_date` matches `FieldNames.forMetadata("signature_date")` (non-alphanumerics → `_`, lowercased, `tika_metadata_` prefix). Names consistent across tasks.
