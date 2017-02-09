package org.icij.extract.extractor;

import org.apache.poi.poifs.filesystem.*;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.Document;
import org.icij.extract.document.EmbeddedDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

/**
 * A custom extractor that saves all embeds to temporary files and records the new paths. It does this
 * non-recursively, so only embedded documents one level deep are linked.
 *
 * Ideally {@link #parseEmbedded} would use {@link org.apache.tika.metadata.TikaCoreProperties.EmbeddedResourceType}
 * but only the PDF parser seems to support it as of Tika 1.8.
 *
 * @since 1.0.0-beta
 */
public class EmbedLinker implements EmbeddedDocumentExtractor {

	/**
	 * Logger for logging exceptions.
	 */
	private static final Logger logger = LoggerFactory.getLogger(EmbedLinker.class);

	private final Document parent;
	private final TemporaryResources tmp;
	private final String open;
	private final String close;

	private int untitled = 0;

	EmbedLinker(final Document parent, final TemporaryResources tmp, final String open, final String close) {
		this.parent = parent;
		this.tmp = tmp;
		this.open = open;
		this.close = close;
	}

	/**
	 * Always returns true. Files are not actually parsed. They are exported.
	 *
	 * @param metadata metadata
	 */
	@Override
	public boolean shouldParseEmbedded(final Metadata metadata) {
		return true;
	}

	@Override
	public void parseEmbedded(final InputStream input, final ContentHandler handler, final Metadata metadata, final
	boolean outputHtml) throws SAXException, IOException {
		String name = metadata.get(Metadata.RESOURCE_NAME_KEY);

		if (null == name || name.isEmpty()) {
			name = String.format("untitled file %d", ++untitled);
		}

		final EmbeddedDocument embed = saveEmbedded(name, input, metadata);

		// If outputHtml is false then it means that the parser already outputted markup for the embed.
		if (outputHtml) {
			final AttributesImpl attributes = new AttributesImpl();

			attributes.addAttribute("", "class", "class", "CDATA", "package-entry");
			handler.startElement(XHTML, "div", "div", attributes);
		}

		final AttributesImpl attributes = new AttributesImpl();
		final String type = metadata.get(Metadata.CONTENT_TYPE);
		final String path = embed.getPath().toString();

		attributes.addAttribute("", "href", "href", "CDATA", open + path + close);
		attributes.addAttribute("", "title", "title", "CDATA", name);
		attributes.addAttribute("", "download", "download", "CDATA", name);

		if (null != type) {
			attributes.addAttribute("", "type", "type", "CDATA", type);
		}

		final char[] chars = name.toCharArray();

		handler.startElement(XHTML, "a", "a", attributes);
		handler.characters(chars, 0, chars.length);
		handler.endElement(XHTML, "a", "a");

		if (outputHtml) {
			handler.endElement(XHTML, "div", "div");
		}
	}

	private EmbeddedDocument saveEmbedded(final String name, final InputStream input, final Metadata metadata) throws
			IOException {
		final Path path = tmp.createTemporaryFile().toPath();

		// Add the embedded document to the parent with a key (which is the temporary path) so that it can be looked
		// up later.
		final EmbeddedDocument embed = parent.addEmbed(path.toString(), (document)-> name, path, metadata);

		if ((input instanceof TikaInputStream) && ((TikaInputStream) input).getOpenContainer() != null && (
				(TikaInputStream) input).getOpenContainer() instanceof DirectoryEntry) {
			final POIFSFileSystem fs = new POIFSFileSystem();

			saveEntries((DirectoryEntry) ((TikaInputStream) input).getOpenContainer(), fs.getRoot());

			try (final OutputStream output = Files.newOutputStream(path)) {
				fs.writeFilesystem(output);
			}

			return embed;
		}

		final long copied;

		try {
			copied = Files.copy(input, path, StandardCopyOption.REPLACE_EXISTING);
		} finally {
			input.close();
		}

		if (copied > 0) {
			logger.info("Copied {} bytes from embedded document \"{}\" in \"{}\" to file.",
					copied, name, parent);
		} else {
			logger.warn("No bytes copied for embedded document \"{}\" in \"{}\". "
					+ "This could indicate a downstream error.", name, parent);
		}

		return embed;
	}

	private void saveEntries(final DirectoryEntry source, final DirectoryEntry destination) throws IOException {
		for (Entry entry : source) {

			// Recursively save sub-entries.
			if (entry instanceof DirectoryEntry) {
				saveEntries((DirectoryEntry) entry, destination.createDirectory(entry.getName()));
				continue;
			}

			// Copy the entry.
			try (final InputStream contents = new DocumentInputStream((DocumentEntry) entry)) {
				destination.createDocument(entry.getName(), contents);
			} catch (IOException e) {
				logger.error("Unable to save embedded document \"{}\" in document: \"{}\".",
						entry.getName(), parent, e);
			}
		}
	}
}
