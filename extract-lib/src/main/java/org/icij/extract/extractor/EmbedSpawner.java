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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.LinkedList;
import java.util.function.Function;

public class EmbedSpawner extends EmbedParser {

	private static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);

	private final Path outputPath;
	private final Function<Writer, ContentHandler> handlerFunction;
	private final LinkedList<TikaDocument> tikaDocumentStack = new LinkedList<>();
	private int untitled = 0;

	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction) {
		super(root, context);
		this.outputPath = outputPath;
		this.handlerFunction = handlerFunction;
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

			if (outputHtml) {
				writeEnd(handler);
			}
		} else {
			// we must not close the input with (try(TikaInputStream.get(input){...}) because
			// it closes the stream and stops tika to get next entries in the PackageParser
			spawnEmbedded(TikaInputStream.get(input), metadata);
		}
	}

	private void spawnEmbedded(final TikaInputStream tis, final Metadata metadata) throws IOException {
		// Create a Writer that will receive the parser output for the embed file, for later retrieval.
		final ByteArrayOutputStream output = new ByteArrayOutputStream(8192);
		final Writer writer = new OutputStreamWriter(output, StandardCharsets.UTF_8);

		final ContentHandler embedHandler = handlerFunction.apply(writer);
		final EmbeddedTikaDocument embed = tikaDocumentStack.getLast().addEmbed(metadata);

		// Use temporary resources to copy formatted output from the content handler to a temporary file.
		// Call setReader on the embed object with a plain reader for this temp file.
		// When all parsing finishes, close temporary resources.
		// Note that getPath should still return the path to the original file.
		embed.setReader(() -> new InputStreamReader(new ByteArrayInputStream(output.toByteArray()), StandardCharsets.UTF_8));

		String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
		if (null == name || name.isEmpty()) {
			name = String.format("untitled_%d", ++untitled);
		}

		try {
			// Trigger spooling of the file to disk so that it can be copied.
			// This needs to be done before parsing starts and the same TIS object must be passed to writeEmbed,
			// otherwise it will be spooled twice.
			if (null != this.outputPath) {
				tis.getPath();
			}
		} catch (final Exception e) {
			logger.error("Unable to spool file to disk (\"{}\" in \"{}\").", name, root, e);

			// If a document can't be spooled then there's a severe problem with the input stream. Abort.
			tikaDocumentStack.getLast().removeEmbed(embed);
			embed.clearReader();
			writer.close();
			return;
		}

		// Add to the stack only immediately before parsing and if there haven't been any fatal errors.
		tikaDocumentStack.add(embed);

		try {
			// Pass the same TIS, otherwise the EmbedParser will attempt to spool the input again and fail, because it's
			// already been consumed.
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
