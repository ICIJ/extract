# Unify the embedded-artifact write layout between EmbedSpawner and EmbeddedDocumentExtractor

Date: 2026-07-13

## Goal

Make the streaming / INDEX-time embedded-file writer (`EmbedSpawner`) persist embedded "raw" artifacts in the same content-addressed on-disk layout that the on-demand / ARTIFACT-stage writer (`EmbeddedDocumentExtractor`) already uses, so both paths agree on where an embed's bytes live.

## Why

In datashare, the artifacts feature (epic #2229, issue #2225) adds a `--artifacts` flag that makes the INDEX stage record a per-document `manifest.json` pointing at each embed's raw payload. That assumes the INDEX parse writes an embed's bytes to the same location the manifest and the serving layer read from. Today it does not: the two extract-lib writers use different layouts, so INDEX-produced manifests would point at bytes written elsewhere. The datashare branch `feat/2225-artifacts-flag` is paused, unmerged, waiting on this extract-lib fix plus a release, after which datashare bumps its extract dependency and resumes.

## Blast-radius analysis (done before design)

### Current layouts (verified)

- TARGET (correct), `EmbeddedDocumentExtractor`:
  - `getEmbeddedPath(artifactPath, id)` = `ArtifactUtils.getEmbeddedPath(artifactPath, id).resolve("raw")`, where `ArtifactUtils.getEmbeddedPath` = `artifactPath/<id[0:2]>/<id[2:4]>/<id>`.
  - Payload -> `artifactPath/xx/yy/<id>/raw`; sidecar -> `.../raw.json` (= `raw` + `.json`), contents `new MetadataTransformer(metadata).transform()`.
- CURRENT (wrong for this purpose), `EmbedSpawner.writeEmbed`:
  - `destination = outputPath.resolve(embed.getHash())` -> flat `outputPath/<hash>`, no `xx/yy` nesting, no `/raw`, no `raw.json` sidecar.

### Who reads the flat `embedOutput/<hash>` path? Nobody.

Grepped all of extract-lib and datashare (all modules, main sources):

- `Extractor.extractPageIndices` (Extractor.java:849) reads `getEmbeddedPath(embedOutput, docId)/pages.json` (the nested layout, a different filename) and creates that directory itself. Unaffected by moving the embed write.
- datashare `SourceExtractor.getEmbeddedSource` reads via `EmbeddedDocumentExtractor.extract(root, document.getId())`, whose cache-hit path is `artifactPath/xx/yy/<id>/raw` + `raw.json` (nested, keyed by `getId()`).
- Nothing reads `embedOutput/<hash>`. `EmbedSpawner`'s flat write is effectively write-only / dead today.

Conclusion: no live reader of the flat layout, so the write can be moved (and re-keyed) with no migration or dual-write. Stale flat files left by prior runs are orphaned and harmless; project deletion removes the whole project artifact dir anyway.

### Critical correction: `getHash()` != `getId()`

The goal asked to confirm `embed.getHash()` equals the digest keying the content-addressed path. It does not:

- `EmbedSpawner.writeEmbed` keys by `embed.getHash()` = `Identifier.retrieveHash(metadata)` = the raw file digest.
- `EmbeddedDocumentExtractor` writes/reads keyed by `embed.getId()` = `DigestIdentifier.generateForEmbed()` = a composite digest of (file digest + parent id + embedded-relationship id + resource name).
- datashare serves by `document.getId()` (the Elasticsearch `_id`), which is that composite id.

Aligning the writers therefore requires re-keying `EmbedSpawner` from `getHash()` to `getId()`, not only moving the path. This is forced by correctness: `getId()` is the only key `SourceExtractor` can find.

### Consequence for existing behavior / tests

`ExtractorTest` (around line 481) asserts the `embeds/` directory holds exactly 6 flat files: dedup-by-content-hash collapses two byte-identical images (`one_pixel_level1.jpg` and `level2/one_pixel.jpg`) into one file. Keyed by `getId()` those two are distinct documents (different name/parent) and produce 7 raw payloads in the nested layout. That test is rewritten to assert the new layout. This is the intended semantics: a per-document manifest needs one payload per indexed embed, not one per unique byte-content.

### Already-built downstream (datashare main)

`ArtifactPath.dir()`, `RawArtifact`, `ArtifactProducer`, `FilesystemManifestRepository` already exist on datashare main and expect `projectRoot/xx/yy/<id>/{raw,raw.json,manifest.json}` keyed by the document id. The ARTIFACT stage already produces this via `EmbeddedDocumentExtractor.extractAll`. This change makes the INDEX stage (`EmbedSpawner`) produce the same.

## Design

### Decisions (confirmed with the user)

1. Key by `embed.getId()` (accepting dedup-by-content -> dedup-by-id, the 6 -> 7 change).
2. Factor the shared logic into a new `EmbeddedArtifactWriter` class.
3. Overwrite on re-write (`REPLACE_EXISTING`), matching `EmbeddedDocumentExtractor`; drop `EmbedSpawner`'s size-compare skip dance.

### New component

`org.icij.extract.extractor.EmbeddedArtifactWriter` is the single owner of the raw on-disk layout:

```java
class EmbeddedArtifactWriter {
    // raw payload at getEmbeddedPath(artifactPath, id)/raw (= artifactPath/xx/yy/<id>/raw)
    // sidecar at <raw> + ".json" (raw.json), contents = new MetadataTransformer(metadata).transform()
    static Path rawPath(Path artifactPath, String id);
    static File write(Path artifactPath, String id, Metadata metadata, Path source) throws IOException;
    static File write(Path artifactPath, String id, Metadata metadata, TikaInputStream tis) throws IOException;
}
```

Both `write` overloads: `Files.createDirectories(parent)`, copy bytes to `raw` (overwrite / `REPLACE_EXISTING`), write `raw.json` from `new MetadataTransformer(metadata).transform()`. The `TikaInputStream` overload calls `tis.reset()` after streaming, matching the current `EmbeddedDocumentExtractor.writeFile(tis)`. This absorbs the logic duplicated today in `EmbeddedDocumentExtractor.writeFile(...)` (both variants).

`EmbeddedDocumentExtractor.getEmbeddedPath(artifactPath, id)` stays as a thin delegate (to `EmbeddedArtifactWriter.rawPath` or the existing `ArtifactUtils.getEmbeddedPath(...).resolve("raw")`) so its callers and tests are untouched.

### Consumers

- `EmbeddedDocumentExtractor`: the two `writeFile` methods become thin delegates to `EmbeddedArtifactWriter.write(...)`. Behavior-identical refactor; existing tests pin the shared contract.
- `EmbedSpawner.writeEmbed`: source-acquisition logic unchanged (the `DirectoryEntry` -> POIFS-temp-file branch for nested Outlook messages, else `tis.getPath()`; the `CONTENT_LENGTH`-if-absent set on metadata). Replace the final `Files.copy(source, outputPath.resolve(embed.getHash()))` block (including the `FileAlreadyExistsException` size-compare handling) with `EmbeddedArtifactWriter.write(outputPath, embed.getId(), metadata, source)`. Net effect: nested layout, keyed by `getId()`, now with the `raw.json` sidecar it currently omits.

### Invariants preserved

`getId()` is computed on the walk thread inside `writeEmbed`, after the embed's digest is present in `metadata` (the same point `getHash()` is read today), so it resolves for both the streaming path and the deferred-OCR path. Untouched: OCR eligibility/deferral, the streaming memory-budget / promise-sink mechanics, digest computation, and the write-failure-must-still-ready-the-sink contract. Both `EmbedSpawner` call sites keep their `try/catch (IOException)` that logs and still readies the sink, so a write failure still cannot leak a promise or hang `awaitDrained()`.

### Error handling

No new failure modes. `EmbeddedArtifactWriter.write` throws `IOException` exactly where the current code does (createDirectories / copy / sidecar write). `EmbeddedDocumentExtractor` callers propagate `IOException` as before. The `EmbedSpawnerSinkTest` F3 regression (embedOutput points at a regular file -> write throws -> sink still readied) keeps working; the failing operation moves from `resolve(hash)`+copy to `createDirectories` under a non-directory, which still throws `IOException`.

## Testing (TDD)

- New `EmbeddedArtifactWriterTest`: raw at `xx/yy/<id>/raw`; sidecar `raw.json` equals `MetadataTransformer(metadata).transform()`; directories auto-created; overwrite-on-rewrite; both overloads (`Path` and `TikaInputStream`, verifying `tis.reset()`).
- `EmbeddedDocumentMemoryExtractorTest` stays green unchanged (pins the refactor as behavior-preserving).
- `ExtractorTest` (around line 481): rewrite to assert the nested layout: walk `embeds/` and assert 7 `raw` payloads (the two byte-identical images are now distinct ids), each with a sibling `raw.json`, instead of 6 flat files.
- New EmbedSpawner-level test: with `embedOutput` set, run an archive with a known embed; assert its bytes land at `getEmbeddedPath(outputPath, embed.getId())/raw`, and that the read path `EmbeddedDocumentExtractor.extract(root, id)` finds them via the cache-hit branch (the cross-writer agreement the goal is about).
- Full `extract-lib` suite green before commit.

## Release

After landing on `master`: bump the version and update the changelog so datashare can bump `org.icij.extract:extract-lib`. Do not push or open a PR without an explicit request.

## Out of scope

- No changes to datashare (its artifact machinery already targets this layout).
- No change to the ARTIFACT-stage path (`EmbeddedDocumentExtractor.extractAll`), only shared-code extraction under it.
- No migration of previously written flat files (no live readers).
