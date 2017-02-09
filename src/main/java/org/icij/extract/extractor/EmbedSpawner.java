package org.icij.extract.extractor;

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
			final Path parsedOutputPath = tmp.createTempFile();
			final String name = metadata.get(Metadata.RESOURCE_NAME_KEY);

			final Writer writer = Files.newBufferedWriter(parsedOutputPath, StandardCharsets.UTF_8);
			final ContentHandler teeHandler = new TeeContentHandler(embedHandler, handlerFunction.apply(writer));

			final EmbeddedDocument embed = stack.getLast().addEmbed(metadata);
			stack.add(embed);

			// Use temporary resources to copy formatted output from the content handler to a temporary file.
			// Call setReader on the embed object with a plain reader for this temp file.
			// When all parsing finishes, close temporary resources.
			// Note that getPath should still return the path to the original file.
			embed.setReader(() -> Files.newBufferedReader(parsedOutputPath, StandardCharsets.UTF_8));

			final Path embedFilePath;

			// Spool the file to disk, if needed, so that it can be copied. This needs to be done before parsing starts.
			if (null != output) {
				embedFilePath = TikaInputStream.get(input, tmp).getPath();
			} else {
				embedFilePath = null;
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
				writer.flush();
				writer.close();
			}

			// Write the embed file to the given output directory.
			if (null != embedFilePath) {
				long copied;

				try (final OutputStream copy = Files.newOutputStream(output.resolve(embed.getId()), StandardOpenOption
						.CREATE_NEW)) {
					copied = Files.copy(embedFilePath, copy);
				} catch (FileAlreadyExistsException e) {
					copied = -1;
				}

				if (0 == copied) {
					logger.warn("No bytes copied for embedded document \"{}\" in \"{}\". "
							+ "This could indicate a downstream error.", name, root);
				} else if (-1 == copied) {
					logger.info("Temporary file for document \"{}\" in \"{}\" already exists.", name, root);
				}
			}
		}

		if (outputHtml) {
			writeEnd(originalHandler);
		}
	}
}
