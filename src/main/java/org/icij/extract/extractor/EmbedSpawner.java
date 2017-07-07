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
import org.icij.extract.document.Document;
import org.icij.extract.document.EmbeddedDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class EmbedSpawner extends EmbedParser {

	private static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);

	private final TemporaryResources tmp;
	private final Path outputPath;
	private final Function<Writer, ContentHandler> handlerFunction;
	private LinkedList<Document> documentStack = new LinkedList<>();
	private int untitled = 0;

	EmbedSpawner(final Document root, final TemporaryResources tmp, final ParseContext context, final Path outputPath,
	             final Function<Writer, ContentHandler> handlerFunction) {
		super(root, context);
		this.tmp = tmp;
		this.outputPath = outputPath;
		this.handlerFunction = handlerFunction;
		documentStack.add(root);
	}

	@Override
	public void parseEmbedded(final InputStream input, final ContentHandler handler, final Metadata metadata,
	                          final boolean outputHtml) throws SAXException, IOException {

		// There's no need to spawn inline embeds, like images in PDFs. These should be concatenated to the main
		// document as usual.
		if (TikaCoreProperties.EmbeddedResourceType.INLINE.toString().equals(metadata
				.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE))) {
			final ContentHandler embedHandler = new EmbeddedContentHandler(new BodyContentHandler(handler));

			if (outputHtml) {
				writeStart(handler, metadata);
			}

			delegateParsing(input, embedHandler, metadata);

			if (outputHtml) {
				writeEnd(handler);
			}
		} else {
			spawnEmbedded(input, metadata);
		}
	}

	private void spawnEmbedded(final InputStream input, final Metadata metadata) throws
			IOException {
		final ByteArrayOutputStream output = new ByteArrayOutputStream(8192);

		// Create a Writer that will receive the parser outputPath for the embed file, for later retrieval.
		final Writer writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(output, true),
				StandardCharsets.UTF_8));

		final ContentHandler embedHandler = handlerFunction.apply(writer);
		final EmbeddedDocument embed = documentStack.getLast().addEmbed(metadata);
		documentStack.add(embed);

		// Use temporary resources to copy formatted outputPath from the content handler to a temporary file.
		// Call setReader on the embed object with a plain reader for this temp file.
		// When all parsing finishes, close temporary resources.
		// Note that getPath should still return the path to the original file.
		embed.setReader(() -> new BufferedReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(output
					.toByteArray())))));

		String name = metadata.get(Metadata.RESOURCE_NAME_KEY);
		if (null == name || name.isEmpty()) {
			name = String.format("untitled_%d", ++untitled);
		}

		// Trigger spooling of the file to disk so that it can be copied.
		// This needs to be done before parsing starts and the same TIS object must be passed to writeEmbed,
		// otherwise it will be spooled twice.
		final TikaInputStream tis = TikaInputStream.get(input, tmp);
		if (null != this.outputPath) {
			tis.getPath();
		}

		try {

			// Pass the same TIS, otherwise the EmbedParser will attempt to spool the input again and fail, because it's
			// already been consumed.
			delegateParsing(tis, embedHandler, metadata);
		} catch (Exception e) {

			// Note that even on exception, the document is intentionally NOT removed from the parent.
			logger.error("Unable to parse embedded document: \"{}\" ({}) (in \"{}\").",
					name, metadata.get(Metadata.CONTENT_TYPE), root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
		} finally {
			documentStack.removeLast();
			writer.close();
		}

		// Write the embed file to the given outputPath directory.
		if (null != this.outputPath) {
			writeEmbed(tis, embed, name);
		}
	}

	private void writeEmbed(final TikaInputStream tis, final EmbeddedDocument embed, final String name) throws IOException {
		final Path source;

		final Metadata metadata = embed.getMetadata();
		final Object container = tis.getOpenContainer();

		// If the input is a container, write it to a temporary file so that it can then be copied atomically.
		// This happens with, for example, an Outlook Message that is an attachment of another Outlook Message.
		if (container instanceof DirectoryEntry) {
			final POIFSFileSystem fs = new POIFSFileSystem();

			source = tmp.createTempFile();
			saveEntries((DirectoryEntry) container, fs.getRoot());

			try (final OutputStream output = Files.newOutputStream(source)) {
				fs.writeFilesystem(output);
			}
		} else {
			source = tis.getPath();
		}

		// Set the content-length as it isn't (always?) set by Tika for embeds.
		if (null == metadata.get(Metadata.CONTENT_LENGTH)) {
			metadata.set(Metadata.CONTENT_LENGTH, Long.toString(Files.size(source)));
		}

		// To prevent massive duplication and because the disk is only a storage for underlying date, save using the
		// straight hash as a filename.
		try (final OutputStream copy = Files.newOutputStream(outputPath.resolve(embed.getHash()),
				StandardOpenOption.CREATE_NEW)) {
			Files.copy(source, copy);
		} catch (FileAlreadyExistsException e) {
			logger.info("Temporary file for document \"{}\" in \"{}\" already exists.", name, root);
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
