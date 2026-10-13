# Quiet Benign Extraction Outcomes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop logging expected "no extractable content" outcomes (unsupported binaries, empty files, encrypted docs) at ERROR, and instead stamp each affected document with one queryable metadata marker, while keeping every document indexed exactly as today.

**Architecture:** Introduce a single `NoContentReason` enum holding one metadata key (`X-Extract:no-content-reason`) with three values. Three code paths stamp it: `FallbackParser` (unsupported/empty files with no parser), `EmbedParser` (embedded zero-byte and encrypted files), and `ParsingReaderWithContentHandler` (root-level zero-byte files). No document is dropped; markers replace ERROR logs and exception stacktraces.

**Tech Stack:** Java 21, Apache Tika 3.3.0, JUnit 4, Fest assertions (`org.fest.assertions.Assertions.assertThat`), Maven.

## Global Constraints

- All changes live in `extract-lib` only. No `datashare` code change is required; markers reach the index via Extract's generic metadata path (`MetadataTransformer` indexes all non-block-listed keys).
- Marker key is exactly `X-Extract:no-content-reason`. Values are exactly `unsupported-media-type`, `empty-file`, `encrypted`.
- Documents must remain indexed. Do not remove/skip any embed or root document. Index document count is unchanged.
- Never silence genuine parse failures: a file detected as a real type that fails to parse must still log at ERROR and record `TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM`.
- Commit messages: Conventional Commits (`type(scope): desc`), single line, no body, no co-author or AI attribution trailers.
- Run tests with Maven: `mvn -pl extract-lib -Dtest=<ClassName> test` (add `-q` to reduce noise). Full module: `mvn -pl extract-lib test`.

---

### Task 1: `NoContentReason` marker enum

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/NoContentReason.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/NoContentReasonTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `NoContentReason` enum with constants `UNSUPPORTED_MEDIA_TYPE`, `EMPTY_FILE`, `ENCRYPTED`.
  - `String NoContentReason.METADATA_KEY` = `"X-Extract:no-content-reason"`.
  - `String NoContentReason.value()` returns the marker string.
  - `static void NoContentReason.stamp(org.apache.tika.metadata.Metadata metadata, NoContentReason reason)` sets `METADATA_KEY` to `reason.value()` (single value, overwrites).

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/NoContentReasonTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class NoContentReasonTest {

    @Test
    public void testKeyIsStable() {
        assertThat(NoContentReason.METADATA_KEY).isEqualTo("X-Extract:no-content-reason");
    }

    @Test
    public void testValues() {
        assertThat(NoContentReason.UNSUPPORTED_MEDIA_TYPE.value()).isEqualTo("unsupported-media-type");
        assertThat(NoContentReason.EMPTY_FILE.value()).isEqualTo("empty-file");
        assertThat(NoContentReason.ENCRYPTED.value()).isEqualTo("encrypted");
    }

    @Test
    public void testStampSetsSingleValue() {
        Metadata metadata = new Metadata();
        NoContentReason.stamp(metadata, NoContentReason.ENCRYPTED);
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("encrypted");
        assertThat(metadata.getValues(NoContentReason.METADATA_KEY).length).isEqualTo(1);
    }

    @Test
    public void testStampOverwrites() {
        Metadata metadata = new Metadata();
        NoContentReason.stamp(metadata, NoContentReason.EMPTY_FILE);
        NoContentReason.stamp(metadata, NoContentReason.UNSUPPORTED_MEDIA_TYPE);
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("unsupported-media-type");
        assertThat(metadata.getValues(NoContentReason.METADATA_KEY).length).isEqualTo(1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -Dtest=NoContentReasonTest test`
Expected: FAIL — compilation error, `NoContentReason` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `extract-lib/src/main/java/org/icij/extract/extractor/NoContentReason.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;

/**
 * Records why a document has no extracted content, without treating it as a failure.
 *
 * A single metadata key ({@link #METADATA_KEY}) carries one of the values below, so a single index
 * filter can surface every document that was intentionally not text-extracted, with the reason
 * available to distinguish the cases. The document is still indexed with its metadata; only the
 * extracted body is empty.
 */
public enum NoContentReason {
    /** Non-empty file whose media type has no Tika parser (e.g. .pyc, .mo, web fonts, .wasm). */
    UNSUPPORTED_MEDIA_TYPE("unsupported-media-type"),
    /** Zero-byte file. */
    EMPTY_FILE("empty-file"),
    /** Password-protected document that could not be decrypted. */
    ENCRYPTED("encrypted");

    public static final String METADATA_KEY = "X-Extract:no-content-reason";

    private final String value;

    NoContentReason(final String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /** Stamp the reason onto the given metadata, replacing any previous value. */
    public static void stamp(final Metadata metadata, final NoContentReason reason) {
        metadata.set(METADATA_KEY, reason.value());
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib -Dtest=NoContentReasonTest test`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/NoContentReason.java \
        extract-lib/src/test/java/org/icij/extract/extractor/NoContentReasonTest.java
git commit -m "feat(extractor): add NoContentReason metadata marker"
```

---

### Task 2: A — unsupported/empty files in `FallbackParser`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/FallbackParser.java` (the `parse` method, currently lines 27-54)
- Create: `extract-lib/src/test/java/org/icij/extract/parser/FallbackParserTest.java`
- Modify: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java` (rewrite `testGarbage`, currently lines 182-201)

**Interfaces:**
- Consumes: `NoContentReason` (Task 1). Note `FallbackParser` is in package `org.icij.extract.parser`, so it must `import org.icij.extract.extractor.NoContentReason;`.
- Produces: no new API. Behavioural change only — `FallbackParser.parse` never throws; it stamps a marker and delegates to `EmptyParser`.

- [ ] **Step 1: Write the failing unit test**

Create `extract-lib/src/test/java/org/icij/extract/parser/FallbackParserTest.java`:

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -Dtest=FallbackParserTest test`
Expected: FAIL — `testNonEmptyUnsupportedIsMarkedAndDoesNotThrow` throws `TikaException("Parse error")` (current `ErrorParser` behaviour); marker assertions fail.

- [ ] **Step 3: Modify `FallbackParser.parse`**

In `extract-lib/src/main/java/org/icij/extract/parser/FallbackParser.java`, add the import near the other imports:

```java
import org.icij.extract.extractor.NoContentReason;
```

Replace the body from the `final Parser parser;` declaration through the final `parser.parse(...)` call. The current code is:

```java
		final Parser parser;
		final long size;
		String value = metadata.get(Metadata.CONTENT_LENGTH);

		if (null != value && !value.isEmpty()) {
			size = Long.valueOf(value);
		} else {
			try (final TikaInputStream tis = TikaInputStream.get(stream)) {
				size = tis.getLength();
			}

			metadata.set(Metadata.CONTENT_LENGTH, Long.toString(size));
		}

		// If the file is not empty, throw a parse error.
		// Otherwise, output an empty document.
		if (size > 0) {
			parser = ErrorParser.INSTANCE;
		} else {
			metadata.set(Metadata.CONTENT_TYPE, "application/octet-stream");
			parser = EmptyParser.INSTANCE;
		}

		parser.parse(stream, handler, metadata, context);
```

Replace it with:

```java
		final long size;
		String value = metadata.get(Metadata.CONTENT_LENGTH);

		if (null != value && !value.isEmpty()) {
			size = Long.valueOf(value);
		} else {
			try (final TikaInputStream tis = TikaInputStream.get(stream)) {
				size = tis.getLength();
			}

			metadata.set(Metadata.CONTENT_LENGTH, Long.toString(size));
		}

		// Nothing to extract. Index the document as empty and record why, instead of throwing.
		if (size > 0) {
			NoContentReason.stamp(metadata, NoContentReason.UNSUPPORTED_MEDIA_TYPE);
		} else {
			metadata.set(Metadata.CONTENT_TYPE, "application/octet-stream");
			NoContentReason.stamp(metadata, NoContentReason.EMPTY_FILE);
		}

		EmptyParser.INSTANCE.parse(stream, handler, metadata, context);
```

(`ErrorParser` is no longer referenced; the existing `import org.apache.tika.parser.*;` wildcard needs no edit.)

- [ ] **Step 4: Run unit test to verify it passes**

Run: `mvn -pl extract-lib -Dtest=FallbackParserTest test`
Expected: PASS (2 tests).

- [ ] **Step 5: Rewrite `testGarbage` for the new behaviour**

In `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java`, replace the current `testGarbage` (lines 182-201), which expects an `IOException` caused by `TikaException("Parse error")`, with:

```java
	@Test
	public void testGarbage() throws Throwable {
        //GIVEN
        Extractor extractor = aBasicExtractor();
        //WHEN
        TikaDocument tikaDocument = extractDocument(extractor, "/documents/garbage.bin");
        final String content;
        try (final Reader reader = tikaDocument.getReader()) {
            content = Spewer.toString(reader);
        }
        //THEN: indexed as empty, marked unsupported, no exception
        assertThat(content.trim()).isEmpty();
        Assert.assertEquals("application/octet-stream", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
        assertThat(tikaDocument.getMetadata().get(NoContentReason.METADATA_KEY)).isEqualTo("unsupported-media-type");
	}
```

`ExtractorTest` is in package `org.icij.extract.extractor`, so `NoContentReason` needs no import. `Spewer` is already imported (used by other tests).

- [ ] **Step 6: Run the updated test to verify it passes**

Run: `mvn -pl extract-lib -Dtest=ExtractorTest#testGarbage test`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/FallbackParser.java \
        extract-lib/src/test/java/org/icij/extract/parser/FallbackParserTest.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "feat(extractor): index unsupported/empty files with a marker instead of failing"
```

---

### Task 3: C + D — embedded zero-byte and encrypted files in `EmbedParser`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java` (the catch blocks in `delegateParsing`, currently lines 91-101)
- Create: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedParserMarkerTest.java`

**Interfaces:**
- Consumes: `NoContentReason` (Task 1, same package — no import needed); `EmbedParser(TikaDocument root, ParseContext context, Parser delegatingParser)` package-private constructor; `void EmbedParser.delegateParsing(InputStream, ContentHandler, Metadata)` package-private method.
- Produces: no new API. Behavioural change: `EncryptedDocumentException` → INFO log + `ENCRYPTED` marker, no stacktrace; `ZeroByteFileException` → DEBUG log + `EMPTY_FILE` marker, no stacktrace; other `TikaException` unchanged (ERROR + stacktrace).

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/EmbedParserMarkerTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedParserMarkerTest {

    private static TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/fake-root"));
    }

    /** A delegate parser that always throws the supplied exception. */
    private static Parser throwing(final TikaException toThrow) {
        return new AbstractParser() {
            @Override
            public Set<MediaType> getSupportedTypes(final ParseContext context) {
                return Collections.emptySet();
            }

            @Override
            public void parse(final InputStream stream, final ContentHandler handler,
                              final Metadata metadata, final ParseContext context) throws TikaException {
                throw toThrow;
            }
        };
    }

    private static Metadata parseWith(final TikaException toThrow) throws Exception {
        final Metadata metadata = new Metadata();
        final EmbedParser parser = new EmbedParser(root(), new ParseContext(), throwing(toThrow));
        parser.delegateParsing(new ByteArrayInputStream(new byte[]{1}), new BodyContentHandler(), metadata);
        return metadata;
    }

    @Test
    public void testEncryptedIsMarkedWithoutStacktrace() throws Exception {
        Metadata metadata = parseWith(new EncryptedDocumentException());
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("encrypted");
        assertThat(metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM)).isNull();
    }

    @Test
    public void testZeroByteIsMarkedWithoutStacktrace() throws Exception {
        Metadata metadata = parseWith(new ZeroByteFileException("empty"));
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("empty-file");
        assertThat(metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM)).isNull();
    }

    @Test
    public void testGenuineParseFailureStillRecordsException() throws Exception {
        Metadata metadata = parseWith(new TikaException("boom"));
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isNull();
        assertThat(metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM)).isNotNull();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -Dtest=EmbedParserMarkerTest test`
Expected: FAIL — encrypted and zero-byte currently record a stacktrace and set no marker, so `testEncryptedIsMarkedWithoutStacktrace` and `testZeroByteIsMarkedWithoutStacktrace` fail. `testGenuineParseFailureStillRecordsException` passes already.

- [ ] **Step 3: Modify the catch blocks in `EmbedParser.delegateParsing`**

In `extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java`, add the import near the other Tika exception imports:

```java
import org.apache.tika.exception.ZeroByteFileException;
```

Replace the two existing catch blocks (currently lines 91-101):

```java
		} catch (final EncryptedDocumentException e) {
			logger.error("Unable to decrypt encrypted document embedded in document: \"{}\" ({}) (in \"{}\").",
					metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY), metadata.get(Metadata.CONTENT_TYPE), root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
		} catch (final TikaException e) {
			logger.error("Unable to parse embedded document: \"{}\" ({}) (in \"{}\").",
					metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY), metadata.get(Metadata.CONTENT_TYPE), root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
		}
```

with (order matters: the `ZeroByteFileException` catch must precede the generic `TikaException` catch, since it is a subclass):

```java
		} catch (final EncryptedDocumentException e) {
			logger.info("Encrypted document not extracted: \"{}\" ({}) (in \"{}\").",
					metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY), metadata.get(Metadata.CONTENT_TYPE), root);
			NoContentReason.stamp(metadata, NoContentReason.ENCRYPTED);
		} catch (final ZeroByteFileException e) {
			logger.debug("Empty embedded document not extracted: \"{}\" ({}) (in \"{}\").",
					metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY), metadata.get(Metadata.CONTENT_TYPE), root);
			NoContentReason.stamp(metadata, NoContentReason.EMPTY_FILE);
		} catch (final TikaException e) {
			logger.error("Unable to parse embedded document: \"{}\" ({}) (in \"{}\").",
					metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY), metadata.get(Metadata.CONTENT_TYPE), root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib -Dtest=EmbedParserMarkerTest test`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedParserMarkerTest.java
git commit -m "feat(extractor): mark embedded empty/encrypted docs instead of logging errors"
```

---

### Task 4: C — root-level zero-byte files in `ParsingReaderWithContentHandler`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ParsingReaderWithContentHandler.java` (the `run()` catch block, currently lines 297-299)
- Modify: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java` (add one test)

**Interfaces:**
- Consumes: `NoContentReason` (Task 1). `ParsingReaderWithContentHandler` is in package `org.icij.extract.parser`, so it must `import org.icij.extract.extractor.NoContentReason;`. `ZeroByteFileException` is already imported (line 19). The `metadata` field is in scope inside `run()`.
- Produces: no new API. A root-level document whose parse throws `ZeroByteFileException` gets the `EMPTY_FILE` marker; read behaviour (EOF) is unchanged.

- [ ] **Step 1: Write the failing test**

In `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java`, add this test (near `testGarbage`). It uses the existing `TemporaryFolder folder` rule to create a genuinely empty file:

```java
	@Test
	public void testEmptyRootFileIsMarked() throws Throwable {
        //GIVEN a zero-byte file
        Path empty = folder.newFile("empty.bin").toPath();
        //WHEN
        TikaDocument tikaDocument = aBasicExtractor().extract(empty);
        final String content;
        try (final Reader reader = tikaDocument.getReader()) {
            content = Spewer.toString(reader);
        }
        //THEN: indexed as empty, marked empty-file, no exception thrown
        assertThat(content.trim()).isEmpty();
        assertThat(tikaDocument.getMetadata().get(NoContentReason.METADATA_KEY)).isEqualTo("empty-file");
	}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -Dtest=ExtractorTest#testEmptyRootFileIsMarked test`
Expected: FAIL — no marker is set today (the `ZeroByteFileException` is caught in `run()` and only recorded as `throwable`), so the assertion on `NoContentReason.METADATA_KEY` fails (null).

- [ ] **Step 3: Modify the `run()` catch block**

In `extract-lib/src/main/java/org/icij/extract/parser/ParsingReaderWithContentHandler.java`, add the import near the other imports:

```java
import org.icij.extract.extractor.NoContentReason;
```

Replace the catch block in `run()` (currently lines 297-299):

```java
			} catch (Throwable t) {
				throwable = t;
			}
```

with (stamp before assigning `throwable`, so the marker is set by the time a reader observes EOF):

```java
			} catch (Throwable t) {
				if (t instanceof ZeroByteFileException) {
					NoContentReason.stamp(metadata, NoContentReason.EMPTY_FILE);
				}
				throwable = t;
			}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib -Dtest=ExtractorTest#testEmptyRootFileIsMarked test`
Expected: PASS.

- [ ] **Step 5: Run the full module test suite (regression)**

Run: `mvn -pl extract-lib test`
Expected: PASS. In particular `testEncryptedPdf` (root-level encrypted, unchanged path) still passes, and no other test regressed.

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ParsingReaderWithContentHandler.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "feat(extractor): mark root-level empty files with a no-content reason"
```

---

## Rollout (outside this plan's code scope)

These changes take effect in the running application only after Extract is released and Datashare
consumes it: bump `extract.version` in Datashare's root `pom.xml` (currently `9.12.0`) to the new
Extract release. No other Datashare change is needed — the marker is indexed by the generic
metadata path. Markers apply only to newly extracted documents; already-indexed documents are
unchanged unless re-extracted. No backfill is in scope.

## Notes for the implementer

- Log-level changes (ERROR→INFO/DEBUG) are verified by the metadata assertions in the tests, not by
  capturing log output (the suite does not assert on logs). The absence of
  `TIKA_META_EXCEPTION_EMBEDDED_STREAM` is the observable proxy for "not treated as an error".
- `EncryptedDocumentException` and `ZeroByteFileException` both extend `TikaException`; keep their
  catch clauses before the generic `TikaException` clause or the file will not compile.
- Do not add `AutoDetectParserConfig.setThrowOnZeroBytes(false)`: it would swallow the zero-byte
  case inside Tika and leave no hook to stamp the marker (see the design doc's rationale).
