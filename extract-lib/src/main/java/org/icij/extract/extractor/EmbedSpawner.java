package org.icij.extract.extractor;

import org.apache.poi.poifs.filesystem.*;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.utils.ExceptionUtils;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.ocr.OCRParser;
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

import static java.lang.Boolean.parseBoolean;

public class EmbedSpawner extends EmbedParser {

	private static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);

	private final Path outputPath;
	private final Function<Writer, ContentHandler> handlerFunction;
	private final LinkedList<TikaDocument> tikaDocumentStack = new LinkedList<>();
	private int untitled = 0;
	private final long embedMemoryBudgetBytes;
	private final TemporaryResources tmp;
	private final BooleanSupplier memoryPressureHigh;
	private final AtomicLong reserved = new AtomicLong();

	private final ExecutorService ocrExecutor;
	private final ExtractionProgress progress;
	private final DigestingParser.Digester digester;
	private final boolean ocrFanout;
	private final long ocrMinImageBytes;

	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh) {
		// Serial mode: no fan-out. All embeds go through the synchronous spawnEmbedded path.
		this(root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh,
				null, null, null, false, 0L);
	}

	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh,
				 final ExecutorService ocrExecutor,
				 final ExtractionProgress progress,
				 final DigestingParser.Digester digester,
				 final boolean ocrFanout, final long ocrMinImageBytes) {
		super(root, context);
		this.outputPath = outputPath;
		this.handlerFunction = handlerFunction;
		this.embedMemoryBudgetBytes = embedMemoryBudgetBytes;
		this.tmp = tmp;
		this.memoryPressureHigh = memoryPressureHigh;
		this.ocrExecutor = ocrExecutor;
		this.progress = progress;
		this.digester = digester;
		this.ocrFanout = ocrFanout;
		this.ocrMinImageBytes = ocrMinImageBytes;
		tikaDocumentStack.add(root);
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
			// it closes the stream and stops tika to get next entries in the PackageParser
			if (ocrFanout && ocrExecutor != null && isOcrEligible(metadata, ocrMinImageBytes)) {
				spawnEmbeddedDeferred(TikaInputStream.get(input), metadata);
			} else {
				spawnEmbedded(TikaInputStream.get(input), metadata);
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
		final EmbeddedTikaDocument embed = tikaDocumentStack.getLast().addEmbed(metadata);

		// The reader is generated lazily during the spew walk, reading from memory or the
		// temp file depending on whether this embed spilled.
		embed.setReader(buffer.readerGenerator());

		String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
		if (null == name || name.isEmpty()) {
			name = String.format("untitled_%d", ++untitled);
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
	}

	// Eligible image attachment: build the embed node, name, digest and (optional) artifact file
	// synchronously on the walk thread so the embed ID, content hash and artifact filename are
	// byte-identical to serial mode, then defer ONLY the OCR text parse to the shared pool.
	private void spawnEmbeddedDeferred(final TikaInputStream tis, final Metadata metadata) throws IOException {
		final BudgetedEmbedBuffer buffer =
				new BudgetedEmbedBuffer(reserved, embedMemoryBudgetBytes, tmp, memoryPressureHigh);
		final Writer writer = new OutputStreamWriter(buffer, StandardCharsets.UTF_8);
		final ContentHandler embedHandler = handlerFunction.apply(writer);
		final EmbeddedTikaDocument embed = tikaDocumentStack.getLast().addEmbed(metadata);

		String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
		if (null == name || name.isEmpty()) {
			name = String.format("untitled_%d", ++untitled);
		}

		// Spool the bytes now (the stream is only valid during this call), then copy them into a
		// temp file owned by the SHARED per-parse TemporaryResources (tmp). This is the critical
		// spool-lifetime fix: tis.getPath() returns tis's OWN transient spool, which is deleted the
		// moment the container parser (PST/zip/PackageParser) advances to the next entry and closes
		// tis. But the OCR task below runs ASYNCHRONOUSLY on the shared pool and reads its input
		// later, long after tis is gone — reading tis's spool directly hits NoSuchFileException and
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
			return;
		}

		// Digest synchronously so the embed ID and artifact filename are identical to serial mode.
		// NOTE: this digests the RAW bytes (now read from the parse-owned copy, which is byte-identical
		// to the spool). That is byte-identical to serial mode ONLY because no
		// org.apache.tika.extractor.EmbeddedStreamTranslator is on the classpath (verified). If one
		// is ever added, serial mode would digest the TRANSLATED bytes, so this eager raw-bytes digest
		// (and thus the artifact filename) could diverge — revisit then.
		try (InputStream digestStream = Files.newInputStream(ocrInput)) {
			digester.digest(digestStream, metadata, context);
		} catch (final Exception e) {
			logger.error("Unable to digest embedded image \"{}\" (in \"{}\").", name, root, e);
		}

		// Back the reader with the OCR future so the spew walk blocks until text is ready (backstop;
		// Extractor also joins all futures before spew).
		final CompletableFuture<Void> done = new CompletableFuture<>();
		embed.setReader(() -> {
			join(done);
			return buffer.readerGenerator().generate();
		});

		// Write the embed artifact file now, while the stream/container is still valid.
		if (null != this.outputPath) {
			writeEmbed(tis, embed, name);
		}

		final String embedName = name;
		try {
			ocrExecutor.submit(() -> {
				try (InputStream in = Files.newInputStream(ocrInput)) {
					delegateParsing(TikaInputStream.get(in), embedHandler, metadata);
				} catch (final Throwable t) {
					logger.error("Deferred OCR failed for \"{}\" (in \"{}\").", embedName, root, t);
					metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
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
