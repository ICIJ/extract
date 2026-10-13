# Unify Embedded-Artifact Write Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `EmbedSpawner` (INDEX-time embed writer) persist embed "raw" bytes in the same content-addressed layout, keyed by the same id, as `EmbeddedDocumentExtractor` (on-demand / ARTIFACT-stage writer), so both writers agree on where an embed's bytes live.

**Architecture:** Extract the shared path+sidecar write logic into a new `EmbeddedArtifactWriter` class. `EmbeddedDocumentExtractor` delegates to it (behavior-preserving refactor). `EmbedSpawner.writeEmbed` is re-pointed to it and re-keyed from `embed.getHash()` (raw file digest) to `embed.getId()` (composite embed id), producing `artifactPath/xx/yy/<id>/raw` plus the `raw.json` sidecar it currently omits.

**Tech Stack:** Java 17, Maven, Apache Tika, JUnit 4, fest-assert (`org.fest.assertions.Assertions.assertThat`), JUnit `TemporaryFolder`.

## Global Constraints

- Java 17; module `extract-lib`. Build/test with Maven.
- Run a single test class/method: `mvn -q -pl extract-lib -am test -Dtest=ClassName#method` from the repo root.
- No em dashes in code or comments.
- Commit via the `/commit` skill; semantic-tag messages; no Claude attribution / co-author trailers.
- Fast-forward / rebase, no squash, keep all commits. Do not push or open a PR without an explicit request.
- The content-addressed layout is: payload at `ArtifactUtils.getEmbeddedPath(artifactPath, id).resolve("raw")` (= `artifactPath/<id[0:2]>/<id[2:4]>/<id>/raw`); sidecar at that path + `.json` (`raw.json`); sidecar contents = `new MetadataTransformer(metadata).transform()` bytes in `Charset.defaultCharset()`.
- Key the layout by `embed.getId()` (composite), never `embed.getHash()` (raw file digest). This is what `EmbeddedDocumentExtractor` writes and datashare's `SourceExtractor` reads.
- Overwrite on re-write (`StandardCopyOption.REPLACE_EXISTING`), matching `EmbeddedDocumentExtractor`.
- Preserve all existing `EmbedSpawner` behavior: OCR eligibility/deferral, streaming memory-budget / promise-sink mechanics, digest computation, the `DirectoryEntry` -> POIFS temp-file source path, the `CONTENT_LENGTH`-if-absent metadata set, and the write-failure-must-still-ready-the-sink guarantee.

---

### Task 1: `EmbeddedArtifactWriter` (new shared writer)

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedArtifactWriter.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedArtifactWriterTest.java`

**Interfaces:**
- Consumes: `ArtifactUtils.getEmbeddedPath(Path, String)` (existing, package-private, returns `artifactPath/xx/yy/<id>`); `org.icij.spewer.MetadataTransformer` (existing; `new MetadataTransformer(metadata).transform()` returns `String`).
- Produces:
  - `static Path rawPath(Path artifactPath, String id)` -> `artifactPath/xx/yy/<id>/raw`
  - `static File write(Path artifactPath, String id, Metadata metadata, Path source) throws IOException`
  - `static File write(Path artifactPath, String id, Metadata metadata, TikaInputStream tis) throws IOException`
  - All three used by Task 2 and Task 3.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedArtifactWriterTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.icij.spewer.MetadataTransformer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.fest.assertions.Assertions.assertThat;

public class EmbeddedArtifactWriterTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static final String ID = "abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890";

    private Metadata metadataWithName(String name) {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return metadata;
    }

    @Test
    public void test_rawPath_is_content_addressed_with_raw_leaf() {
        Path root = tmp.getRoot().toPath();
        assertThat(EmbeddedArtifactWriter.rawPath(root, ID))
                .isEqualTo(root.resolve("ab").resolve("cd").resolve(ID).resolve("raw"));
    }

    @Test
    public void test_write_from_path_creates_raw_and_sidecar() throws Exception {
        Path root = tmp.getRoot().toPath();
        Path source = tmp.newFile("payload.bin").toPath();
        Files.write(source, "hello".getBytes());
        Metadata metadata = metadataWithName("payload.bin");

        File written = EmbeddedArtifactWriter.write(root, ID, metadata, source);

        assertThat(written).isEqualTo(EmbeddedArtifactWriter.rawPath(root, ID).toFile());
        assertThat(written).isFile();
        assertThat(Files.readAllBytes(written.toPath())).isEqualTo("hello".getBytes());
        File sidecar = new File(written + ".json");
        assertThat(sidecar).isFile();
        assertThat(Files.readAllBytes(sidecar.toPath()))
                .isEqualTo(new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
    }

    @Test
    public void test_write_from_tika_input_stream_creates_raw_and_sidecar_and_resets() throws Exception {
        Path root = tmp.getRoot().toPath();
        Path source = tmp.newFile("stream.bin").toPath();
        Files.write(source, "streamed".getBytes());
        Metadata metadata = metadataWithName("stream.bin");

        try (TikaInputStream tis = TikaInputStream.get(source)) {
            File written = EmbeddedArtifactWriter.write(root, ID, metadata, tis);
            assertThat(Files.readAllBytes(written.toPath())).isEqualTo("streamed".getBytes());
            assertThat(new File(written + ".json")).isFile();
            // reset() was called: the stream is readable again from the start.
            assertThat(tis.read()).isEqualTo((int) 's');
        }
    }

    @Test
    public void test_write_overwrites_existing_payload() throws Exception {
        Path root = tmp.getRoot().toPath();
        Path first = tmp.newFile("first.bin").toPath();
        Files.write(first, "one".getBytes());
        Path second = tmp.newFile("second.bin").toPath();
        Files.write(second, "twotwo".getBytes());
        Metadata metadata = metadataWithName("x");

        EmbeddedArtifactWriter.write(root, ID, metadata, first);
        File written = EmbeddedArtifactWriter.write(root, ID, metadata, second);

        assertThat(Files.readAllBytes(written.toPath())).isEqualTo("twotwo".getBytes());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl extract-lib -am test -Dtest=EmbeddedArtifactWriterTest`
Expected: compilation failure / FAIL — `EmbeddedArtifactWriter` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Create `extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedArtifactWriter.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.icij.spewer.MetadataTransformer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

// Single owner of the content-addressed raw layout for an embedded document's bytes.
// Both writers (the streaming EmbedSpawner and the on-demand EmbeddedDocumentExtractor) go
// through here so their on-disk layout, sidecar format and key can never drift apart.
// The layout is: payload at getEmbeddedPath(artifactPath, id)/raw and sidecar at raw.json,
// keyed by the embed id (NOT the raw file digest), so a manifest and the serving layer that
// resolve by id find the bytes this class writes.
class EmbeddedArtifactWriter {

    private EmbeddedArtifactWriter() {}

    // artifactPath/<id[0:2]>/<id[2:4]>/<id>/raw
    static Path rawPath(final Path artifactPath, final String id) {
        return ArtifactUtils.getEmbeddedPath(artifactPath, id).resolve("raw");
    }

    // Copy already-spooled bytes into the raw payload (overwriting) and write the raw.json sidecar.
    static File write(final Path artifactPath, final String id, final Metadata metadata, final Path source) throws IOException {
        final File embedded = rawPath(artifactPath, id).toFile();
        Files.createDirectories(Paths.get(embedded.getParent()));
        Files.copy(source, embedded.toPath(), StandardCopyOption.REPLACE_EXISTING);
        try (FileOutputStream metadataOutputStream = new FileOutputStream(embedded + ".json")) {
            metadataOutputStream.write(new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
        }
        return embedded;
    }

    // Stream the embed bytes into the raw payload (8K buffer) and write the raw.json sidecar, then
    // reset the stream so the caller can reuse it. Mirrors EmbeddedDocumentExtractor's prior writeFile(tis).
    static File write(final Path artifactPath, final String id, final Metadata metadata, final TikaInputStream tis) throws IOException {
        final File embedded = rawPath(artifactPath, id).toFile();
        Files.createDirectories(Paths.get(embedded.getParent()));
        try (FileOutputStream embeddedOutputStream = new FileOutputStream(embedded);
             FileOutputStream metadataOutputStream = new FileOutputStream(embedded + ".json")) {
            int nbTmpBytesRead;
            for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = tis.read(tmp)) > 0; ) {
                embeddedOutputStream.write(tmp, 0, nbTmpBytesRead);
            }
            metadataOutputStream.write(new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
        }
        tis.reset();
        return embedded;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl extract-lib -am test -Dtest=EmbeddedArtifactWriterTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedArtifactWriter.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedArtifactWriterTest.java
git commit -m "feat(artifacts): add EmbeddedArtifactWriter for the shared raw layout"
```

(Use the `/commit` skill to author the message; the text above is the intended content.)

---

### Task 2: Route `EmbeddedDocumentExtractor` through `EmbeddedArtifactWriter`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java` (method `getEmbeddedPath` at ~120-122; the two `writeFile` methods at ~228-251)
- Test (existing, must stay green, do not edit): `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentMemoryExtractorTest.java`

**Interfaces:**
- Consumes: `EmbeddedArtifactWriter.rawPath(...)`, `EmbeddedArtifactWriter.write(...)` (both overloads) from Task 1.
- Produces: no signature change. `EmbeddedDocumentExtractor.getEmbeddedPath(Path, String)` still returns the `.../raw` path; the two `protected File writeFile(...)` overloads keep their signatures.

This is a behavior-preserving refactor: the existing `EmbeddedDocumentMemoryExtractorTest` (which asserts payloads land at `getEmbeddedPath(...)` and that content is byte-correct) is the regression gate. No new test.

- [ ] **Step 1: Confirm the regression gate passes before the change**

Run: `mvn -q -pl extract-lib -am test -Dtest=EmbeddedDocumentMemoryExtractorTest`
Expected: PASS. (Establishes the green baseline this refactor must preserve.)

- [ ] **Step 2: Delegate `getEmbeddedPath` to the shared writer**

In `EmbeddedDocumentExtractor.java`, replace the body of `getEmbeddedPath` (currently `return ArtifactUtils.getEmbeddedPath(artifactPath, digest).resolve("raw");`):

```java
    static Path getEmbeddedPath(Path artifactPath, String digest) {
        return EmbeddedArtifactWriter.rawPath(artifactPath, digest);
    }
```

- [ ] **Step 3: Delegate both `writeFile` overloads to the shared writer**

Replace the two `writeFile` methods (the `TikaInputStream` variant and the `Path source` variant) with:

```java
        protected File writeFile(Metadata metadata, String digest, TikaInputStream tis) throws IOException {
            return EmbeddedArtifactWriter.write(this.artifactPath, digest, metadata, tis);
        }

        protected File writeFile(Metadata metadata, String digest, Path source) throws IOException {
            return EmbeddedArtifactWriter.write(this.artifactPath, digest, metadata, source);
        }
```

Then remove imports in `EmbeddedDocumentExtractor.java` that are now unused ONLY IF the compiler flags them (candidates: `java.io.FileOutputStream`, `java.nio.charset.Charset`, `java.nio.file.Paths`, `java.nio.file.StandardCopyOption`). Keep any still referenced elsewhere in the file (e.g. `Files`, `StandardCopyOption` may still be used by the translation block near line 199; `Paths` may be used elsewhere). Let compilation drive removal; do not remove blindly.

- [ ] **Step 4: Run the regression gate to verify it still passes**

Run: `mvn -q -pl extract-lib -am test -Dtest=EmbeddedDocumentMemoryExtractorTest`
Expected: PASS (unchanged behavior).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java
git commit -m "refactor(artifacts): route EmbeddedDocumentExtractor through EmbeddedArtifactWriter"
```

---

### Task 3: Re-point and re-key `EmbedSpawner.writeEmbed`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java` (method `writeEmbed` at ~780-819)
- Modify (test): `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java` (method `testEmbeddedWithDuplicates` at ~451-482)

**Interfaces:**
- Consumes: `EmbeddedArtifactWriter.write(Path, String, Metadata, Path)` from Task 1; `EmbeddedDocumentExtractor.getEmbeddedPath(Path, String)` (Task 2) in the test; `TikaDocument.getId()`, `TikaDocument.apply(Consumer)`, `TikaDocument.getEmbeds()` (existing).
- Produces: `EmbedSpawner.writeEmbed` now writes to `getEmbeddedPath(outputPath, embed.getId())/raw` + `raw.json`. No signature change.

- [ ] **Step 1: Rewrite the failing test to assert the new layout**

In `ExtractorTest.java`, replace the body of `testEmbeddedWithDuplicates` (keep the `@Test` annotation and method signature). The new test derives the expected embed ids from the document tree (no magic file count) and proves each embed's bytes live at its `getId()`-keyed path with a `raw.json` sidecar:

```java
    @Test
    public void testEmbeddedWithDuplicates() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
        extractor.disableOcr();
        Path embedsRoot = folder.newFolder("embeds").toPath();
        extractor.setEmbedOutputPath(embedsRoot);
        /*
        embedded_with_duplicate.tgz contains a nested tree in which two images
        (level1/one_pixel_level1.jpg and level1/level2/one_pixel.jpg) are byte-identical.
        Keyed by embed id (parent + relationship + name + file digest) they are DISTINCT
        documents, so each gets its own raw payload -- unlike the old flat, hash-keyed layout
        that collapsed them into one file.
        */
        //WHEN
        TikaDocument tikaDocument = extractDocument(extractor, "/documents/embedded_with_duplicate.tgz");
        FileSpewer fileSpewer = new FileSpewer(new FieldNames());
        fileSpewer.setOutputDirectory(folder.getRoot().toPath());
        fileSpewer.write(tikaDocument);

        //THEN
        // Collect every embed id in the tree (skip the root; only embeds are written).
        List<String> embedIds = new ArrayList<>();
        tikaDocument.apply(doc -> {
            if (doc != tikaDocument) {
                embedIds.add(doc.getId());
            }
        });
        // Every embed's bytes live at its id-keyed raw path, with a raw.json sidecar next to it.
        for (String id : embedIds) {
            Path raw = EmbeddedDocumentExtractor.getEmbeddedPath(embedsRoot, id);
            assertThat(raw.toFile()).isFile();
            assertThat(raw.resolveSibling("raw.json").toFile()).isFile();
        }
        // The number of raw payloads on disk equals the number of distinct embed ids
        // (the two identical images are distinct ids, so they are NOT deduplicated).
        long rawFilesOnDisk;
        try (Stream<Path> walk = Files.walk(embedsRoot)) {
            rawFilesOnDisk = walk.filter(p -> p.getFileName().toString().equals("raw")).count();
        }
        assertThat(toIntExact(rawFilesOnDisk)).isEqualTo((int) embedIds.stream().distinct().count());
    }
```

(`java.util.ArrayList`, `java.util.List`, `java.nio.file.Path`, `java.nio.file.Files`, `java.util.stream.Stream`, and `toIntExact` are already imported in `ExtractorTest.java`.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl extract-lib -am test -Dtest=ExtractorTest#testEmbeddedWithDuplicates`
Expected: FAIL — `EmbedSpawner` still writes flat `outputPath/<hash>` files, so `getEmbeddedPath(embedsRoot, id)` is not a file (no `xx/yy/<id>/raw`, no `raw.json`).

- [ ] **Step 3: Re-point and re-key `writeEmbed`**

In `EmbedSpawner.java`, replace the whole `writeEmbed` method body. Keep the `DirectoryEntry` -> POIFS temp-file source acquisition, the `else source = tis.getPath()` branch, and the `CONTENT_LENGTH`-if-absent set exactly as they are; only the destination path + copy changes (from flat `outputPath.resolve(embed.getHash())` with the `FileAlreadyExistsException` size-compare dance, to the shared writer keyed by `embed.getId()`):

```java
	private void writeEmbed(final TikaInputStream tis, final EmbeddedTikaDocument embed, final String name) throws IOException {
		final Path source;

		final Metadata metadata = embed.getMetadata();
		final Object container = tis.getOpenContainer();

		// If the input is a container, write it to a temporary file so that it can then be copied atomically.
		// This happens with, for example, an Outlook Message that is an attachment of another Outlook Message.
		if (container instanceof DirectoryEntry) {
			try (final TemporaryResources tmp = new TemporaryResources();
			     final POIFSFileSystem fs = new POIFSFileSystem()) {
				source = tmp.createTempFile();
				saveEntries((DirectoryEntry) container, fs.getRoot());

				try (final OutputStream output = Files.newOutputStream(source)) {
					fs.writeFilesystem(output);
				}
			}
		} else {
			source = tis.getPath();
		}

		// Set the content-length as it isn't (always?) set by Tika for embeds.
		if (null == metadata.get(Metadata.CONTENT_LENGTH)) {
			metadata.set(Metadata.CONTENT_LENGTH, Long.toString(Files.size(source)));
		}

		// Write the raw payload and its raw.json sidecar in the content-addressed layout, keyed by the
		// embed id (NOT the raw file digest). This is the same layout and key EmbeddedDocumentExtractor
		// uses, so an id-addressed manifest and datashare's on-demand SourceExtractor resolve to these
		// exact bytes. The name is used only for the log breadcrumb on failure (see the call sites).
		EmbeddedArtifactWriter.write(outputPath, embed.getId(), metadata, source);
	}
```

- [ ] **Step 4: Remove now-unused imports in `EmbedSpawner.java` (only if flagged)**

`java.nio.file.FileAlreadyExistsException` and `java.nio.file.StandardCopyOption` were used only by the old copy dance. Remove them if the compiler reports them unused. Do NOT remove `java.nio.file.*`-covered symbols still in use elsewhere in the file (the file uses `import java.nio.file.*;` at the top, so there may be nothing to remove — let compilation decide).

- [ ] **Step 5: Run the rewritten test to verify it passes**

Run: `mvn -q -pl extract-lib -am test -Dtest=ExtractorTest#testEmbeddedWithDuplicates`
Expected: PASS.

- [ ] **Step 6: Run the EmbedSpawner and Extractor test suites for regressions**

Run: `mvn -q -pl extract-lib -am test -Dtest=EmbedSpawner*,ExtractorTest,EmbeddedDocumentMemoryExtractorTest`
Expected: PASS (in particular `EmbedSpawnerSinkTest`'s F3 write-failure-still-readies-sink regression: the failing operation is now `Files.createDirectories(...)` under a non-directory `embedOutput`, which still throws `IOException`, so the sink is still readied).

- [ ] **Step 7: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "feat(artifacts): EmbedSpawner writes raw/raw.json keyed by embed id"
```

---

### Task 4: Cross-writer agreement test

**Files:**
- Modify (test): `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java` (add one new `@Test`)

**Interfaces:**
- Consumes: `Extractor.setEmbedOutputPath`, `Extractor.extract`; `EmbeddedDocumentExtractor(UpdatableDigester, Path)` + `extract(TikaDocument, String)` (existing); `UpdatableDigester` (existing in package). Proves the bytes `EmbedSpawner` wrote during extraction are found by `EmbeddedDocumentExtractor.extract` via its cache-hit branch keyed by the embed id.

- [ ] **Step 1: Write the failing test**

Add to `ExtractorTest.java` (near `testEmbeddedWithDuplicates`). This picks one first-level embed produced by `EmbedSpawner`, then reads it back through `EmbeddedDocumentExtractor` (the datashare serving path) using the same artifact dir, and asserts the content matches:

```java
    @Test
    public void testEmbedSpawnerWriteIsReadableByEmbeddedDocumentExtractor() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
        extractor.disableOcr();
        Path artifactDir = folder.newFolder("artifacts").toPath();
        extractor.setEmbedOutputPath(artifactDir);

        //WHEN: EmbedSpawner writes each embed's raw bytes under artifactDir during extraction.
        TikaDocument tikaDocument = extractDocument(extractor, "/documents/embedded_with_duplicate.tgz");
        FileSpewer fileSpewer = new FileSpewer(new FieldNames());
        fileSpewer.setOutputDirectory(folder.getRoot().toPath());
        fileSpewer.write(tikaDocument);

        // Pick a first-level embed that actually has bytes on disk.
        EmbeddedTikaDocument embed = tikaDocument.getEmbeds().stream()
                .filter(e -> EmbeddedDocumentExtractor.getEmbeddedPath(artifactDir, e.getId()).toFile().isFile())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no first-level embed was written to the artifact dir"));
        byte[] expected = Files.readAllBytes(EmbeddedDocumentExtractor.getEmbeddedPath(artifactDir, embed.getId()));

        //THEN: the on-demand reader (datashare's serving path) resolves the same id to the same bytes,
        // via EmbeddedDocumentExtractor.extract's cache-hit branch (no re-parse needed).
        EmbeddedDocumentExtractor reader = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), artifactDir);
        TikaDocumentSource source = reader.extract(tikaDocument, embed.getId());
        assertThat(source.getContent()).isEqualTo(expected);
    }
```

Add imports to `ExtractorTest.java` if missing: `org.icij.extract.document.TikaDocumentSource`. (`EmbeddedTikaDocument` is already imported; `UpdatableDigester` is in the same package as the test, so no import needed.)

- [ ] **Step 2: Run the test to verify it fails, then passes**

If Task 3 is already implemented, this test should PASS immediately (it documents and locks the cross-writer contract). To see it fail first, you may temporarily stash the Task 3 change; otherwise treat this as a characterization test.

Run: `mvn -q -pl extract-lib -am test -Dtest=ExtractorTest#testEmbedSpawnerWriteIsReadableByEmbeddedDocumentExtractor`
Expected: PASS (with Task 3 applied).

Note: the on-demand reader's cache-hit branch (`EmbeddedDocumentExtractor.extract`, lines ~95-100) matches purely on `getEmbeddedPath(artifactPath, id)` existing and reads `raw` + `raw.json`; it does not re-derive the id, so the digester passed to the reader need only be able to construct the extractor. The id equality is what this test proves.

- [ ] **Step 3: Commit**

```bash
git add extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "test(artifacts): EmbedSpawner output is readable by EmbeddedDocumentExtractor"
```

---

### Task 5: Full suite green + release prep

**Files:**
- Modify: `pom.xml` (root, `<version>` at line 7), `extract-lib/pom.xml` (`<version>` / parent version at line 8), and any other module pom carrying the version.
- Modify: `CHANGELOG` (find the changelog file with `ls` / `git log`; follow the existing format).

**Interfaces:** none (build metadata only).

- [ ] **Step 1: Run the full extract-lib test suite**

Run: `mvn -q -pl extract-lib -am test`
Expected: PASS. (The Makefile notes the full `make test` may require a running Redis; the touched tests here do not. If unrelated Redis-dependent tests fail for infra reasons, confirm the artifact/embed tests above pass and report the infra-only failures to the maintainer rather than "fixing" them.)

- [ ] **Step 2: Stop and confirm with the maintainer before bumping the version**

Do not bump the version or edit the changelog until the maintainer confirms the change has landed on `master` (house rule: prepare the release after it lands). Ask: "extract-lib change is green and committed. Ready for me to bump the version and update the changelog for a release datashare can depend on?"

- [ ] **Step 3 (after confirmation): Bump version and changelog**

Current version is `9.15.3` (root `pom.xml:7`). Bump the patch (to `9.15.4`) across the poms that carry it (root `pom.xml`, `extract-lib/pom.xml` parent block, and any sibling module). Add a changelog entry in the existing style describing: "EmbedSpawner writes embedded raw/raw.json in the content-addressed layout keyed by embed id, matching EmbeddedDocumentExtractor."

- [ ] **Step 4: Commit the release**

```bash
git add pom.xml extract-lib/pom.xml CHANGELOG
git commit -m "release: 9.15.4"
```

(Do not push or tag without an explicit request from the maintainer.)

---

## Self-Review

**Spec coverage:**
- New `EmbeddedArtifactWriter` factoring shared logic -> Task 1. ✓
- `EmbeddedDocumentExtractor` delegates (behavior-preserving) -> Task 2. ✓
- `EmbedSpawner` re-pointed to nested layout + `raw.json`, keyed by `getId()` -> Task 3. ✓
- Overwrite semantics (`REPLACE_EXISTING`) -> Task 1 (`write(Path)`), used by Task 3. ✓
- `ExtractorTest` line ~481 rewrite (6 flat -> 7 nested, per-id) -> Task 3 Step 1. ✓
- Cross-writer agreement (EmbedSpawner output found by EmbeddedDocumentExtractor read path) -> Task 4. ✓
- Preserve OCR/deferral/sink/digest/DirectoryEntry/CONTENT_LENGTH behavior -> Task 3 Step 3 keeps those blocks verbatim; Task 3 Step 6 regression-runs the EmbedSpawner suites. ✓
- Sink write-failure-still-readies regression -> Task 3 Step 6 (EmbedSpawnerSinkTest F3). ✓
- Release prep, no push without asking -> Task 5. ✓
- No em dashes: verified across all code/comment blocks in this plan. ✓

**Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to Task N". Every code step shows complete code. ✓

**Type consistency:** `EmbeddedArtifactWriter.rawPath(Path,String)`, `write(Path,String,Metadata,Path)`, `write(Path,String,Metadata,TikaInputStream)` are defined in Task 1 and referenced with those exact signatures in Tasks 2, 3, 4. `EmbeddedDocumentExtractor.getEmbeddedPath(Path,String)` keeps its signature (Task 2) and is used in Tasks 3-4 tests. `embed.getId()` (not `getHash()`) is the key throughout. ✓
