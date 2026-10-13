# Design: quiet benign extraction outcomes with a queryable marker

## Problem

Extraction logs a large volume of `ERROR` lines that are not Extract failures: they are
per-embedded-file outcomes on files that have nothing to extract. In one observed CLI run,
these dominated the log. Three categories account for the bulk:

- **Unsupported binaries** (~58% of errors in the sample): non-empty files with no real Tika
  parser — `.pyc`, `.mo`, web fonts (`.woff/.woff2/.eot/.otf`), `.wasm`, `.glb`, `.swf`,
  certificates, etc. `FallbackParser` deliberately throws `ErrorParser`'s "Parse error" for
  every non-empty file it cannot parse.
- **Zero-byte files**: empty files that get routed by name/extension to a real parser and throw
  `ZeroByteFileException`.
- **Encrypted documents**: password-protected files that cannot be decrypted.

In every case the document is *already indexed* (Extract keeps the embed and spews it; the
extracted text is simply empty). The noise is therefore pure log clutter plus an exception
stacktrace recorded into the document metadata. Real errors are drowned out.

## Goal

Stop logging these expected outcomes at `ERROR`, and instead stamp each affected document with a
single, queryable metadata marker recording *why* it has no extracted content. Keep every one of
these documents indexed exactly as today (document count in the index is unchanged).

Non-goals:

- No change to whether these files are indexed. They stay indexed.
- No backfill of documents already in the index.
- No OCR changes, no archive-parallelism changes (tracked separately).

## Design

### Shared marker: `NoContentReason`

A new enum in `org.icij.extract.extractor` centralises the marker key and its allowed values:

```java
public enum NoContentReason {
    UNSUPPORTED_MEDIA_TYPE("unsupported-media-type"), // A
    EMPTY_FILE("empty-file"),                         // C
    ENCRYPTED("encrypted");                           // D

    public static final String METADATA_KEY = "X-Extract:no-content-reason";

    private final String value;
    NoContentReason(String value) { this.value = value; }
    public String value() { return value; }

    public static void stamp(Metadata metadata, NoContentReason reason) {
        metadata.set(METADATA_KEY, reason.value());
    }
}
```

One key with three values means a single index filter surfaces every non-extracted document,
with the reason available to distinguish them.

**Index visibility:** Extract's `MetadataTransformer` iterates all metadata names and indexes
everything not on `metadata_block_list` (a block-list, not an allow-list). `X-Extract:no-content-reason`
is not blocked, so it is indexed and queryable in Datashare with no Datashare code change.

### A — unsupported non-empty binaries

File: `extract-lib/src/main/java/org/icij/extract/parser/FallbackParser.java`

Current behaviour (`parse`, the `size > 0` branch): use `ErrorParser.INSTANCE`, which throws
`TikaException("Parse error")`.

New behaviour, both branches delegate to `EmptyParser.INSTANCE` (never `ErrorParser`), stamping a
marker first:

- `size > 0` → stamp `NoContentReason.UNSUPPORTED_MEDIA_TYPE`.
- `size == 0` → stamp `NoContentReason.EMPTY_FILE` (an empty file that reached the fallback because
  no parser matched its type).

Result: document indexed with metadata, empty body, no exception thrown, no `ERROR` log, no
stacktrace recorded — plus the queryable marker.

Note on `EMPTY_FILE`: it is stamped in two distinct code paths that both mean "empty file" — here
in `FallbackParser` (empty file, no matching parser) and in the C path below (empty file routed to
a real parser that throws `ZeroByteFileException`). Both set the same marker value, so the index
sees them uniformly.

### C — zero-byte files

`ZeroByteFileException extends TikaException`, so today it is caught by the generic
`catch (TikaException)` in `EmbedParser.delegateParsing` (`EmbedParser.java:96`) and logged at
`ERROR` via the shared `EmbedParser` logger (used by `EmbedSpawner` too).

New behaviour: add a specific `catch (ZeroByteFileException)` *before* the generic
`catch (TikaException)` that:

- stamps `NoContentReason.EMPTY_FILE`,
- logs at `DEBUG` (effectively silent in production),
- does not record the exception stacktrace into metadata.

The root-level path (`ParsingReaderWithContentHandler.java:249`, which already special-cases
`ZeroByteFileException`) gets the same marker stamp.

The specific-catch approach is chosen over Tika's `AutoDetectParserConfig.setThrowOnZeroBytes(false)`
precisely because we want the marker: the config path swallows the case inside Tika and gives no
hook to stamp anything.

### D — encrypted documents

File: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java` (the existing
`catch (EncryptedDocumentException)` at `:91`).

New behaviour:

- stamp `NoContentReason.ENCRYPTED`,
- log at `INFO` (down from `ERROR`) — encrypted files are actionable for an investigator, so keep
  a low-volume, visible line,
- drop the `TIKA_META_EXCEPTION_EMBEDDED_STREAM` stacktrace recording (it only ever says
  "document is encrypted").

The document is still indexed with its metadata, as today.

## Testing

Following existing patterns (`EmbedSpawner*Test`, `ExtractorTest`), fixtures in
`extract-lib/src/test/resources/documents/`:

- **A** — new `FallbackParserTest`: non-empty unknown-type stream produces an empty document (no
  exception) with `no-content-reason = unsupported-media-type`; empty stream produces an empty
  document.
- **C** — `EmbedSpawner`-level test with a fixture archive containing a zero-byte entry (e.g. an
  empty `.html`) → embed indexed, marker `= empty-file`, no `ERROR` logged.
- **D** — test with an encrypted fixture → embed indexed, marker `= encrypted`, logged at `INFO`,
  no stacktrace recorded in metadata.
- **Regression guard** — a genuinely broken non-empty file (wrong magic bytes) still surfaces as a
  real parse failure, confirming actual errors are not silenced.

## Rollout

- All code lands in `extract-lib` only.
- Markers reach the index via the generic metadata path; no Datashare code change required.
- Consumption is routine: release Extract, then bump `extract.version` in Datashare's root
  `pom.xml` (currently `9.12.0`).
- Markers apply only to newly extracted documents. Files already in the index are unchanged unless
  re-extracted. No backfill in scope.
