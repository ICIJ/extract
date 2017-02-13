package org.icij.extract.extractor;

import org.apache.poi.poifs.filesystem.*;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.sax.TeeContentHandler;
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

public class EmbedSpawner extends EmbedParser {

	private static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);

	private final TemporaryResources tmp;
	private final Path output;
	private ContentHandler originalHandler = null;
	private final Function<Writer, ContentHandler> handlerFunction;
	private LinkedList<Document> stack = new LinkedList<>();
	private int untitled = 0;

	EmbedSpawner(final Document root, final TemporaryResources tmp, final ParseContext context, final Path output,
	             final Function<Writer, ContentHandler> handlerFunction) {
		super(root, context);
		this.tmp = tmp;
		this.output = output;
		this.handlerFunction = handlerFunction;
		stack.add(root);
	}

	@Override
	public void parseEmbedded(final InputStream input, final ContentHandler handler, final Metadata metadata,
	                          final boolean outputHtml) throws SAXException, IOException {

		// Need to keep track of and use the original handler, since a modified one is passed to the parser.
		if (null == originalHandler) {
			originalHandler = handler;
		}

		// Use a different handler for receiving SAX events from the embedded document. The allows the main content
		// handler that receives the entire concatenated content to receive only the body of the embed, while the
		// handler that writes to the temporary file will receive the entire document.
		final ContentHandler embedHandler = new EmbeddedContentHandler(new BodyContentHandler(originalHandler));

		if (outputHtml) {
			writeStart(originalHandler, metadata);
		}

		// There's no need to spawn inline embeds, like images in PDFs. These should be concatenated to the main
		// document as usual.
		if (TikaCoreProperties.EmbeddedResourceType.INLINE.toString().equals(metadata
				.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE))) {
			delegateParsing(input, embedHandler, metadata);
		} else {
			spawnEmbedded(input, embedHandler, metadata);
		}

		if (outputHtml) {
			writeEnd(originalHandler);
		}
	}

	private void spawnEmbedded(final InputStream input, final ContentHandler handler, final Metadata metadata) throws
			IOException {
		final Path parsedOutputPath = tmp.createTempFile();
		String name = metadata.get(Metadata.RESOURCE_NAME_KEY);

		final Writer writer = Files.newBufferedWriter(parsedOutputPath, StandardCharsets.UTF_8);
		final ContentHandler teeHandler = new TeeContentHandler(handler, handlerFunction.apply(writer));

		final EmbeddedDocument embed = stack.getLast().addEmbed(metadata);
		stack.add(embed);

		// Use temporary resources to copy formatted output from the content handler to a temporary file.
		// Call setReader on the embed object with a plain reader for this temp file.
		// When all parsing finishes, close temporary resources.
		// Note that getPath should still return the path to the original file.
		embed.setReader(() -> Files.newBufferedReader(parsedOutputPath, StandardCharsets.UTF_8));

		if (null == name || name.isEmpty()) {
			name = String.format("untitled_%d", ++untitled);
		}

		// Trigger spooling of the file to disk so that it can be copied.
		// This needs to be done before parsing starts.
		if (null != output) {
			TikaInputStream.get(input, tmp).getFile();
		}

		try {
			delegateParsing(input, teeHandler, metadata);
		} catch (Exception e) {

			// Note that even on exception, the document is intentionally NOT removed from the parent.
			logger.error("Unable to parse embedded document: \"{}\" (in \"{}\").", name, root, e);

			// TODO: Change to TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM in Tika 1.15.
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_WARNING, ExceptionUtils.getFilteredStackTrace(e));
		} finally {
			stack.removeLast();
			writer.close();
		}

		// Write the embed file to the given output directory.
		if (null != output) {
			writeEmbed(input, embed, name);
		}
	}

	private void writeEmbed(final InputStream input, final EmbeddedDocument embed, final String name) throws IOException {
		final Path source;

		final Metadata metadata = embed.getMetadata();
		final TikaInputStream tis = TikaInputStream.get(input, tmp);
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
		try (final OutputStream copy = Files.newOutputStream(output.resolve(embed.getHash()),
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
