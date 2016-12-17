package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.icij.extract.document.Document;
import org.icij.extract.document.EmbeddedDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.function.Function;


public class EmbedSpawner extends EmbedParser {

	private static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);

	private final TemporaryResources tmp;
	private ContentHandler originalHandler = null;
	private final Function<Writer, ContentHandler> handlerFunction;
	private LinkedList<Document> documentStack = new LinkedList<>();

	EmbedSpawner(final Document rootDocument, final ParseContext context, final TemporaryResources tmp, final
	Function<Writer, ContentHandler> handlerFunction) {
		super(rootDocument, context);
		this.tmp = tmp;
		this.handlerFunction = handlerFunction;
		documentStack.add(rootDocument);
	}

	// TODO: if an output directory is given, 1) copy the file to a file on disk 2) parse that file to the output
	// stream (handler) using the delegating parser. Output filename doesn't matter since the parser will use the
	// RESOURCE_NAME_KEY from metadata to detect.

	// Use temporary resources to copy formatted output from the content handler to a temporary file.
	// Call setReader on the embed object with a plain reader for this temp file
	// When all parsing finishes, close temporary resources
	// Note that getPath should still return

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
			final Path tmpPath = tmp.createTempFile();

			final Writer writer = Files.newBufferedWriter(tmpPath, StandardCharsets.UTF_8);
			final ContentHandler teeHandler = new TeeContentHandler(embedHandler, handlerFunction.apply(writer));

			final EmbeddedDocument embed = documentStack.getLast().addEmbed(metadata);
			documentStack.add(embed);

			embed.setReader(() -> Files.newBufferedReader(tmpPath, StandardCharsets.UTF_8));

			try {
				delegateParsing(input, teeHandler, metadata);
			} catch (Exception e) {
				logger.error(String.format("Unable to parse embedded document in document: \"%s\".", rootDocument), e);
			} finally {
				documentStack.removeLast();
				writer.flush();
				writer.close();
			}
		}

		if (outputHtml) {
			writeEnd(originalHandler);
		}
	}
}
