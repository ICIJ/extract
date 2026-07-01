package org.icij.extract.extractor;

import org.apache.poi.poifs.filesystem.*;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.extractor.DocumentSelector;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.utils.ExceptionUtils;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.ocr.OCRParser;
import org.icij.spewer.SpewItem;
import org.icij.spewer.SpewSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.parser.DigestingParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.Boolean.parseBoolean;

public class EmbedSpawner extends EmbedParser {

	private static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);

	private final Path outputPath;
	private final Function<Writer, ContentHandler> handlerFunction;
	private final LinkedList<TikaDocument> tikaDocumentStack = new LinkedList<>();
	// Per-parent monotonic ordinal for nameless non-inline embeds: each gets an order-independent
	// name from its immediate parent id + its 0-based index among that parent's nameless children.
	// A map (not a reset-on-change slot) so a parent revisited after a nested descent keeps counting.
	private final java.util.Map<String, Integer> untitledOrdinalsByParent = new java.util.HashMap<>();
	private final long embedMemoryBudgetBytes;
	private final TemporaryResources tmp;
	private final BooleanSupplier memoryPressureHigh;
	private final AtomicLong reserved;

	// Supplies the OCR executor lazily; called only when an eligible image embed is deferred.
	// The no-fan-out legacy constructor passes () -> null with ocrEnabled=false so this path
	// is never reached without an actual executor.
	private final Supplier<ExecutorService> ocrExecutorSupplier;
	private final boolean ocrEnabled;
	private final ExtractionProgress progress;
	private final DigestingParser.Digester digester;
	private final boolean ocrFanout;
	private final long ocrMinImageBytes;
	// Class name of the configured OCR parser (e.g. org.apache.tika.parser.ocr.TesseractOCRParser).
	// Set SYNCHRONOUSLY on the shared embed metadata by the deferred path so OCR_PARSER is present
	// before spew and identical to serial mode, never written from the pool thread. May be null when
	// OCR is disabled (in which case the deferred path is never reached).
	private final String ocrParserClassName;

	// Streaming spew sink; null in the legacy buffer-then-walk mode. When set, each embed is
	// promised at creation and readied when its text is buffered (synchronously for non-deferred
	// embeds, on the OCR completion thread for deferred ones).
	private final SpewSink sink;

	// Same translator EmbeddedDocumentExtractor uses: delegates to all service-registered
	// EmbeddedStreamTranslator implementations (e.g. MSEmbeddedStreamTranslator from
	// tika-parser-microsoft-module), so this stays future-proof rather than hardcoded to MS.
	private final org.apache.tika.extractor.EmbeddedStreamTranslator streamTranslator =
			new org.apache.tika.extractor.DefaultEmbeddedStreamTranslator();

	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh) {
		// Serial mode: no fan-out. All embeds go through the synchronous spawnEmbedded path.
		// Pass a no-op supplier and ocrEnabled=false so the deferred path is never reached.
		this(root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh,
				() -> null, false, null, null, false, 0L, null, null);
	}

	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh,
				 final ExecutorService ocrExecutor,
				 final ExtractionProgress progress,
				 final DigestingParser.Digester digester,
				 final boolean ocrFanout, final long ocrMinImageBytes,
				 final String ocrParserClassName) {
		// Backward-compatible overload: wrap the live executor in a supplier so existing call sites
		// (including tests) that pass a pre-built ExecutorService continue to work unchanged.
		this(root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh,
				() -> ocrExecutor, ocrExecutor != null, progress, digester, ocrFanout, ocrMinImageBytes,
				ocrParserClassName, null);
	}

	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh,
				 final Supplier<ExecutorService> ocrExecutorSupplier,
				 final boolean ocrEnabled,
				 final ExtractionProgress progress,
				 final DigestingParser.Digester digester,
				 final boolean ocrFanout, final long ocrMinImageBytes,
				 final String ocrParserClassName,
				 final SpewSink sink) {
		super(root, context);
		this.outputPath = outputPath;
		this.handlerFunction = handlerFunction;
		this.embedMemoryBudgetBytes = embedMemoryBudgetBytes;
		this.tmp = tmp;
		this.memoryPressureHigh = memoryPressureHigh;
		this.ocrExecutorSupplier = ocrExecutorSupplier;
		this.ocrEnabled = ocrEnabled;
		this.progress = progress;
		this.digester = digester;
		this.ocrFanout = ocrFanout;
		this.ocrMinImageBytes = ocrMinImageBytes;
		this.ocrParserClassName = ocrParserClassName;
		this.sink = sink;
		this.reserved = new AtomicLong();
		tikaDocumentStack.add(root);
	}

	// Shares every collaborator AND the live memory budget with `template`, but gets its own DFS
	// stack (seeded with the shared root) and its own per-parent untitled state. Used by the PST
	// folder fan-out so each walker has an isolated parent stack while the global embed-memory
	// budget stays shared across all walkers.
	private EmbedSpawner(final EmbedSpawner template) {
		super(template.root, forkContext(template.context));
		this.outputPath = template.outputPath;
		this.handlerFunction = template.handlerFunction;
		this.embedMemoryBudgetBytes = template.embedMemoryBudgetBytes;
		this.tmp = template.tmp;
		this.memoryPressureHigh = template.memoryPressureHigh;
		this.ocrExecutorSupplier = template.ocrExecutorSupplier;
		this.ocrEnabled = template.ocrEnabled;
		this.progress = template.progress;
		this.digester = template.digester;
		this.ocrFanout = template.ocrFanout;
		this.ocrMinImageBytes = template.ocrMinImageBytes;
		this.ocrParserClassName = template.ocrParserClassName;
		this.sink = template.sink;
		this.reserved = template.reserved; // SHARED budget
		// Register THIS fork as the extractor in its OWN context so nested embeds (message attachments,
		// depth>=2) recurse into this fork's DFS stack + untitled map, not the shared base spawner.
		this.context.set(EmbeddedDocumentExtractor.class, this);
		tikaDocumentStack.add(template.root); // fresh stack, seeded with the shared root
	}

	public EmbedSpawner fork() {
		return new EmbedSpawner(this);
	}

	// Test accessors (package-private).
	AtomicLong reservedBudget() { return reserved; }
	int stackDepth() { return tikaDocumentStack.size(); }

	// Deterministic, order-independent name for a non-inline embed that has no resource name.
	// Derived from the immediate parent id + the embed's ordinal among its parent's children, so
	// the parallel index and the serial on-demand walk produce the same name for the same embed.
	static String untitledName(final String parentId, final int siblingOrdinal) {
		return "untitled_" + parentId + "_" + siblingOrdinal;
	}

	@Override
	public void parseEmbedded(final InputStream input, final ContentHandler handler, final Metadata metadata,
	                          final boolean outputHtml) throws SAXException, IOException {

		// There's no need to spawn inline embeds, like images in PDFs. These should be concatenated to the main
		// document as usual.
		if (TikaCoreProperties.EmbeddedResourceType.INLINE.toString().equals(metadata.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE))) {
			final ContentHandler embedHandler = new EmbeddedContentHandler(new BodyContentHandler(handler));

			if (outputHtml) {
				writeStart(handler, metadata);
			}

			delegateParsing(input, embedHandler, metadata);

            // If OCR was used for this embedded leaf item, bubble up to the parent document metadata only
            String ocrParser = metadata.get(OCRParser.OCR_PARSER);
            if (ocrParser != null) {
                tikaDocumentStack.getLast().getMetadata().set(OCRParser.OCR_PARSER, ocrParser);
            }

			if (outputHtml) {
				writeEnd(handler);
			}
		} else {
			if (progress != null) {
				progress.incrementEmbeds();
			}
			// we must not close the input with (try(TikaInputStream.get(input){...}) because
			// it closes the stream and stops tika to get next entries in the PackageParser.
			// Wrap once and reuse for whichever path runs: shouldTranslate() inspects the stream's
			// type (instanceof TikaInputStream) and its open container, so it MUST see this tis.
			final TikaInputStream tis = TikaInputStream.get(input);
			// Defer (parallel OCR off the walk thread) ONLY when the embed is OCR-eligible AND the
			// translator would NOT rewrite its bytes. A translatable embed (e.g. an image stored as
			// an OLE object inside a legacy .doc/.xls/.ppt) is digested over its TRANSLATED bytes by
			// Tika's DigestingParser in serial mode, so it must take the serial inline path here too,
			// or its embed ID / X-TIKA:digest:* / artifact filename would diverge between modes.
			// Defer only when fanout is on, OCR is enabled, the embed is an eligible image, and
			// the stream translator would not rewrite its bytes (translatable embeds must take
			// the serial path so their digest matches serial mode).
			if (ocrFanout && ocrEnabled && isOcrEligible(metadata, ocrMinImageBytes)
					&& !streamTranslator.shouldTranslate(tis, metadata)) {
				spawnEmbeddedDeferred(tis, metadata);
			} else {
				spawnEmbedded(tis, metadata);
			}
		}
	}

	private void spawnEmbedded(final TikaInputStream tis, final Metadata metadata) throws IOException {
		// Buffer the embed's extracted text in memory while the global budget allows,
		// overflowing to a temp file once exceeded, so multi-GB containers (PSTs, zips,
		// mailboxes) don't retain the whole tree's text in heap at once.
		final BudgetedEmbedBuffer buffer = new BudgetedEmbedBuffer(reserved, embedMemoryBudgetBytes, tmp, memoryPressureHigh);
		final Writer writer = new OutputStreamWriter(buffer, StandardCharsets.UTF_8);

		final ContentHandler embedHandler = handlerFunction.apply(writer);
		final TikaDocument spewParent = tikaDocumentStack.getLast();
		final int spewLevel = tikaDocumentStack.size();
		if (sink != null) {
			sink.promise();
		}
		final EmbeddedTikaDocument embed = spewParent.addEmbed(metadata);

		// The reader is generated lazily during the spew walk, reading from memory or the
		// temp file depending on whether this embed spilled.
		embed.setReader(buffer.readerGenerator());

		String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
		if (null == name || name.isEmpty()) {
			final String parentId = tikaDocumentStack.getLast().getId();
			final int ordinal = untitledOrdinalsByParent.merge(parentId, 1, Integer::sum) - 1;
			name = untitledName(parentId, ordinal);
		}

		try {
			// Trigger spooling of the file to disk so that it can be copied.
			if (null != this.outputPath) {
				tis.getPath();
			}
		} catch (final Exception e) {
			logger.error("Unable to spool file to disk (\"{}\" in \"{}\").", name, root, e);

			// If a document can't be spooled then there's a severe problem with the input stream. Abort.
			tikaDocumentStack.getLast().removeEmbed(embed);
			embed.clearReader();
			buffer.discard();
			writer.close();
			if (sink != null) {
				// Balance the promise(): this embed will never be readied.
				sink.ready(new SpewItem(embed, spewParent, root, spewLevel));
			}
			return;
		}

		// Add to the stack only immediately before parsing and if there haven't been any fatal errors.
		tikaDocumentStack.add(embed);

		try {
			delegateParsing(tis, embedHandler, metadata);
		} catch (final Exception e) {

			// Note that even on exception, the document is intentionally NOT removed from the parent.
			logger.error("Unable to parse embedded document: \"{}\" ({}) (in \"{}\").",
					name, metadata.get(Metadata.CONTENT_TYPE), root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
		} finally {
			tikaDocumentStack.removeLast();
			writer.close();
		}

		// Write the embed file to the given outputPath directory.
		if (null != this.outputPath) {
			writeEmbed(tis, embed, name);
		}

		// Text is fully buffered and the artifact (if any) is written: hand this embed to the spew worker.
		if (sink != null) {
			sink.ready(new SpewItem(embed, spewParent, root, spewLevel));
		}
	}

	// Eligible image attachment: build the embed node, name, digest and (optional) artifact file
	// synchronously on the walk thread so the embed ID, content hash and artifact filename are
	// byte-identical to serial mode, then defer ONLY the OCR text parse to the shared pool.
	private void spawnEmbeddedDeferred(final TikaInputStream tis, final Metadata metadata) throws IOException {
		final BudgetedEmbedBuffer buffer =
				new BudgetedEmbedBuffer(reserved, embedMemoryBudgetBytes, tmp, memoryPressureHigh);
		final Writer writer = new OutputStreamWriter(buffer, StandardCharsets.UTF_8);
		final ContentHandler embedHandler = handlerFunction.apply(writer);
		final TikaDocument spewParent = tikaDocumentStack.getLast();
		final int spewLevel = tikaDocumentStack.size();
		if (sink != null) {
			sink.promise();
		}
		final EmbeddedTikaDocument embed = spewParent.addEmbed(metadata);

		String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
		if (null == name || name.isEmpty()) {
			final String parentId = tikaDocumentStack.getLast().getId();
			final int ordinal = untitledOrdinalsByParent.merge(parentId, 1, Integer::sum) - 1;
			name = untitledName(parentId, ordinal);
		}

		// Spool the bytes now (the stream is only valid during this call), then copy them into a
		// temp file owned by the SHARED per-parse TemporaryResources (tmp). This is the critical
		// spool-lifetime fix: tis.getPath() returns tis's OWN transient spool, which is deleted the
		// moment the container parser (PST/zip/PackageParser) advances to the next entry and closes
		// tis. But the OCR task below runs ASYNCHRONOUSLY on the shared pool and reads its input
		// later, long after tis is gone: reading tis's spool directly hits NoSuchFileException and
		// the OCR text is silently lost. The shared tmp is closed by ResourceClosingReader only AFTER
		// the full spew completes, and every embed's OCR completes before the spew finishes (the
		// per-embed reader backstop guarantees it), so a tmp-owned copy outlives the async OCR read.
		final Path spooled;
		try {
			spooled = tis.getPath();
		} catch (final Exception e) {
			logger.error("Unable to spool file to disk (\"{}\" in \"{}\").", name, root, e);
			// Severe problem with the input stream. Abort this embed, mirroring spawnEmbedded.
			tikaDocumentStack.getLast().removeEmbed(embed);
			embed.clearReader();
			buffer.discard();
			writer.close();
			if (sink != null) {
				sink.ready(new SpewItem(embed, spewParent, root, spewLevel));
			}
			return;
		}

		// Durable, parse-owned copy of the embed bytes for the async OCR read. Made synchronously on
		// the parse thread while tis is still valid; lifetime spans until the root reader closes
		// (post-spew), outliving the async OCR. Serialize the createTempFile() call: it mutates the
		// shared tmp's resource list, which is also touched by BudgetedEmbedBuffer spill on the pool.
		final Path ocrInput;
		try {
			synchronized (tmp) {
				ocrInput = tmp.createTempFile();
			}
			Files.copy(spooled, ocrInput, StandardCopyOption.REPLACE_EXISTING);
		} catch (final Exception e) {
			logger.error("Unable to copy embedded image to OCR temp file (\"{}\" in \"{}\").", name, root, e);
			// Same severity as a spool failure: abort this embed, mirroring spawnEmbedded.
			tikaDocumentStack.getLast().removeEmbed(embed);
			embed.clearReader();
			buffer.discard();
			writer.close();
			if (sink != null) {
				sink.ready(new SpewItem(embed, spewParent, root, spewLevel));
			}
			return;
		}

		// Digest synchronously so the embed ID and artifact filename are identical to serial mode.
		// NOTE: this digests the RAW bytes (now read from the parse-owned copy, which is byte-identical
		// to the spool). The deferred path only ever sees embeds for which streamTranslator.shouldTranslate()
		// is FALSE: translatable embeds (whose bytes serial mode would digest AFTER translation) are
		// routed to the serial spawnEmbedded path in parseEmbedded. For every embed that reaches here,
		// translation is a no-op, so digesting the raw spooled bytes is byte-identical to serial mode.
		try (InputStream digestStream = Files.newInputStream(ocrInput)) {
			digester.digest(digestStream, metadata, context);
		} catch (final Exception e) {
			logger.error("Unable to digest embedded image \"{}\" (in \"{}\").", name, root, e);
		}

		// Set OCR_PARSER on the SHARED metadata SYNCHRONOUSLY (walk thread), before submit. In serial
		// mode the OCRParserAdapter writes this from inside the parse; here the async parse runs against
		// a private metadata clone and must NOT touch the shared object, so we set it eagerly to the
		// configured OCR parser's class name (deterministic; identical to serial). This guarantees the
		// indexed OCR_PARSER is present before spew, keeping Datashare's on-demand SourceExtractor.useOcr()
		// (which checks the indexed OCR_PARSER) working without any pool-thread write to shared state.
		if (ocrParserClassName != null && metadata.get(OCRParser.OCR_PARSER) == null) {
			metadata.set(OCRParser.OCR_PARSER, ocrParserClassName);
		}

		// ISOLATE the async OCR parse from all shared mutable state:
		//  (a) a PRIVATE clone of the metadata (the pool thread writes only this clone, never the shared
		//      Metadata that the spew thread concurrently reads for getId()/indexing (avoids the
		//      ConcurrentModificationException / corruption data race), and
		//  (b) an ISOLATED ParseContext carrying forward this.context's parsing config but with an
		//      EmbedBlocker as the EmbeddedDocumentExtractor, so a nested embed discovered inside the
		//      image is ignored instead of re-entering parseEmbedded and mutating the shared
		//      tikaDocumentStack from the pool thread.
		final Metadata ocrMeta = new Metadata();
		for (final String n : metadata.names()) {
			for (final String v : metadata.getValues(n)) {
				ocrMeta.add(n, v);
			}
		}
		final ParseContext isolatedContext = buildIsolatedOcrContext();

		// Back the reader with the OCR future so the spew walk blocks until text is ready (backstop;
		// Extractor also joins all futures before spew). On the spew thread (single-threaded per embed
		// once done is joined) we merge any parse-derived fields the async parse added to ocrMeta back
		// into the shared metadata, race-free. We only copy keys ABSENT from the shared metadata, so the
		// synchronously-set id/digest/OCR_PARSER/name are never overwritten.
		// TRADEOFF: parse-derived fields (e.g. image dimensions tiff:ImageWidth/Length) are merged at
		// reader-read time, so a consumer that serializes the embed's metadata strictly BEFORE reading
		// its content may not see them. The index-critical fields (id/digest/OCR_PARSER) are always
		// present because they are set synchronously above. This is acceptable and far better than the
		// prior data race on the shared Metadata.
		final CompletableFuture<Void> done = new CompletableFuture<>();
		embed.setReader(() -> {
			join(done);
			mergeParseDerivedFields(metadata, ocrMeta);
			return buffer.readerGenerator().generate();
		});

		// Stream this embed to the spew worker once its OCR future completes (normal completion, a
		// recorded failure in the task finally, or a rejected submit all complete `done`). The callback
		// runs on the completing thread, so the worker never dequeues a deferred embed before its OCR is
		// done -- it therefore never blocks on OCR.
		if (sink != null) {
			done.whenComplete((v, t) -> sink.ready(new SpewItem(embed, spewParent, root, spewLevel)));
		}

		// Write the embed artifact file now, while the stream/container is still valid.
		if (null != this.outputPath) {
			writeEmbed(tis, embed, name);
		}

		final String embedName = name;
		// Resolve the executor lazily here; for the Extractor-backed path this triggers the
		// synchronized lazy-creation in Extractor.ocrExecutor() on the first deferred embed.
		final ExecutorService resolvedOcrExecutor = ocrExecutorSupplier.get();
		try {
			resolvedOcrExecutor.submit(() -> {
				try (InputStream in = Files.newInputStream(ocrInput)) {
					// Parse into the private clone + isolated context only. No write to shared state.
					delegateParsing(TikaInputStream.get(in), embedHandler, ocrMeta, isolatedContext);
				} catch (final Throwable t) {
					logger.error("Deferred OCR failed for \"{}\" (in \"{}\").", embedName, root, t);
					// Record onto the clone; merged back onto the shared metadata on the spew thread.
					ocrMeta.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
							ExceptionUtils.getFilteredStackTrace(t));
				} finally {
					try {
						writer.close();
					} catch (final IOException ignored) {
						// best-effort: text already buffered
					}
					if (progress != null) {
						progress.incrementOcrCompleted();
					}
					done.complete(null);
				}
				return null;
			});
		} catch (final RejectedExecutionException e) {
			// The executor was shut down mid-parse (e.g. Extractor.close()). The OCR task will never
			// run, so the reader backstop (join(done)) would otherwise block forever. Record the error,
			// release the writer, and complete the backstop so the spew walk never hangs. We only count
			// the submit as "submitted" once it actually succeeds, so the submitted/completed counters
			// stay balanced without touching them here.
			logger.error("Deferred OCR rejected for \"{}\" (in \"{}\").", embedName, root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
			try {
				writer.close();
			} catch (final IOException ignored) {
				// best-effort: nothing actionable on the rejection path
			}
			done.complete(null);
			return;
		}
		// Submit succeeded: now it's safe to count it (keeps submitted/completed balanced).
		if (progress != null) {
			progress.incrementOcrSubmitted();
		}
	}

	// Build a ParseContext for the async OCR parse that carries forward this.context's parsing config
	// (the Parser, OCR/PDF/HTML config and DocumentSelector the parse needs) but replaces the
	// EmbeddedDocumentExtractor with a fresh EmbedBlocker. The blocker makes a nested embed inside the
	// image a no-op (shouldParseEmbedded() returns false), so the pool thread can never re-enter
	// parseEmbedded and mutate the shared tikaDocumentStack. We copy only known-safe keys; notably we
	// do NOT carry over this.context's EmbeddedDocumentExtractor (which IS this EmbedSpawner).
	private ParseContext buildIsolatedOcrContext() {
		final ParseContext isolated = new ParseContext();
		copyParsingConfig(context, isolated);
		// Ignore any nested embed discovered inside the image rather than mutating shared state.
		isolated.set(EmbeddedDocumentExtractor.class, new EmbedBlocker());
		return isolated;
	}

	// Copies the parsing collaborators every derived parse needs (parser + OCR/PDF/HTML config +
	// selector) from `source` into `target`, WITHOUT the EmbeddedDocumentExtractor or PstFanoutConfig.
	// Single maintenance point shared by fork() and buildIsolatedOcrContext(): if Extractor starts
	// setting a new parsing-config key on the context, add it here once.
	private static void copyParsingConfig(final ParseContext source, final ParseContext target) {
		final Parser p = source.get(Parser.class);
		if (p != null) { target.set(Parser.class, p); }
		final TesseractOCRConfig tess = source.get(TesseractOCRConfig.class);
		if (tess != null) { target.set(TesseractOCRConfig.class, tess); }
		final PDFParserConfig pdf = source.get(PDFParserConfig.class);
		if (pdf != null) { target.set(PDFParserConfig.class, pdf); }
		final HtmlMapper html = source.get(HtmlMapper.class);
		if (html != null) { target.set(HtmlMapper.class, html); }
		final DocumentSelector selector = source.get(DocumentSelector.class);
		if (selector != null) { target.set(DocumentSelector.class, selector); }
	}

	// Builds a fork's own ParseContext: a copy of the base parsing config, MINUS the
	// EmbeddedDocumentExtractor (the caller registers `this` after super() so nested embeds recurse
	// into THIS fork's own DFS stack instead of the shared base spawner -- the byte-identity fix) and
	// MINUS PstFanoutConfig (so a nested PST/OST attachment reads null -> canFanOut=false -> serial
	// walk, so only the outermost PST fans out; no reentrant pstParseExecutor starvation).
	private static ParseContext forkContext(final ParseContext base) {
		final ParseContext fork = new ParseContext();
		copyParsingConfig(base, fork);
		return fork;
	}

	// Copy keys present in the async parse's private metadata clone but ABSENT from the shared metadata
	// into the shared metadata. Runs on the spew thread after join(done), so it is single-threaded with
	// respect to this embed and race-free. Never overwrites the synchronously-set id/digest/OCR_PARSER/
	// name fields (they are already present in the shared metadata, so they are skipped here).
	private static void mergeParseDerivedFields(final Metadata shared, final Metadata ocrMeta) {
		for (final String n : ocrMeta.names()) {
			if (shared.get(n) == null) {
				for (final String v : ocrMeta.getValues(n)) {
					shared.add(n, v);
				}
			}
		}
	}

	private static void join(final Future<Void> f) {
		try {
			f.get();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (final ExecutionException ignored) {
			// OCR task already records TIKA_META_EXCEPTION_EMBEDDED_STREAM; never fatal.
		}
	}

	private void writeEmbed(final TikaInputStream tis, final EmbeddedTikaDocument embed, final String name) throws IOException {
		final Path destination = outputPath.resolve(embed.getHash());
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

		// To prevent massive duplication and because the disk is only a storage for underlying data, save using the
		// straight hash as a filename.
		try {
			Files.copy(source, destination);
		} catch (final FileAlreadyExistsException e) {
			if (Files.size(source) != Files.size(destination)) {
				Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
			} else {
				logger.info("Temporary file for document \"{}\" in \"{}\" already exists.", name, root);
			}
		}
	}

	static boolean isOcrEligible(final Metadata metadata, final long minImageBytes) {
		final String contentType = metadata.get(Metadata.CONTENT_TYPE);
		if (contentType == null || !contentType.startsWith("image/")) {
			return false;
		}
		final String resourceType = metadata.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE);
		if (TikaCoreProperties.EmbeddedResourceType.INLINE.toString().equals(resourceType)) {
			return false;
		}
		if (minImageBytes > 0) {
			final String length = metadata.get(Metadata.CONTENT_LENGTH);
			if (length != null) {
				try {
					if (Long.parseLong(length) < minImageBytes) {
						return false;
					}
				} catch (final NumberFormatException ignored) { /* treat as unknown -> eligible */ }
			}
		}
		return true;
	}

	private void saveEntries(final DirectoryEntry source, final DirectoryEntry destination) throws IOException {
		for (Entry entry : source) {

			// Recursively save sub-entries or copy the entry.
			if (entry instanceof DirectoryEntry) {
				saveEntries((DirectoryEntry) entry, destination.createDirectory(entry.getName()));
			} else {
				try (final InputStream contents = new DocumentInputStream((DocumentEntry) entry)) {
					destination.createDocument(entry.getName(), contents);
				}
			}
		}
	}
}
