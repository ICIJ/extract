package org.icij.extract.parser;

import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import com.pff.PSTObject;
import com.pff.PstMessageDescriptors;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PST;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.microsoft.pst.PSTMailItemParser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * A resilient replacement for Tika's {@code OutlookPSTParser}.
 *
 * <p>Tika's parser has no per-message isolation: one failing message aborts the
 * rest of that folder and every subsequent folder, silently losing email. This
 * parser isolates every message and every folder, recovers messages that are
 * not linked into the visible folder hierarchy, and records a reconciliation
 * count (expected vs emitted vs failed) on the root document metadata so any
 * residual loss is visible and alertable.
 *
 * <p>It does not parse message bodies itself: each {@link PSTMessage} is handed
 * to Tika's existing {@code PSTMailItemParser} exactly as the stock parser does,
 * so body and attachment extraction and all metadata mapping are unchanged.
 */
public class ResilientOutlookPSTParser implements Parser {

    public static final MediaType MS_OUTLOOK_PST_MIMETYPE = MediaType.application("vnd.ms-outlook-pst");
    public static final String PST_EXPECTED = "tika:pst_expected";
    public static final String PST_EMITTED = "tika:pst_emitted";
    public static final String PST_FAILED = "tika:pst_failed";

    private static final long serialVersionUID = 1L;
    private static final Set<MediaType> SUPPORTED_TYPES = singleton(MS_OUTLOOK_PST_MIMETYPE);
    private static final Logger logger = LoggerFactory.getLogger(ResilientOutlookPSTParser.class);

    @Override
    public Set<MediaType> getSupportedTypes(final ParseContext context) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
                      final ParseContext context) throws IOException, SAXException, TikaException {
        final EmbeddedDocumentExtractor extractor = EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(context);
        metadata.set(Metadata.CONTENT_TYPE, MS_OUTLOOK_PST_MIMETYPE.toString());

        final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();

        final TikaInputStream in = TikaInputStream.get(stream);
        final String pstPath = in.getFile().getPath();
        final Set<Long> emittedIds = new HashSet<>();
        final int[] emitted = {0};
        PSTFile pstFile = null;
        try {
            pstFile = new PSTFile(pstPath);
            if (pstFile.getPSTFileType() == PSTFile.PST_TYPE_2013_UNICODE) {
                throw new TikaException("OST 2013 is not supported by java-libpst");
            }

            List<Integer> messageDescriptorIds;
            try {
                messageDescriptorIds = PstMessageDescriptors.normalMessageDescriptorIds(pstFile);
            } catch (final Exception e) {
                logger.warn("Could not enumerate PST message descriptors for \"{}\"", pstPath, e);
                messageDescriptorIds = null;
            }
            walkFolder(pstFile.getRootFolder(), "/", xhtml, extractor, emittedIds, emitted);
            if (messageDescriptorIds != null) {
                recoverOrphans(messageDescriptorIds, xhtml, extractor, emittedIds, emitted, pstFile);
            }
            if (messageDescriptorIds != null) {
                final int expected = messageDescriptorIds.size();
                final int failed = Math.max(0, expected - emitted[0]);
                metadata.set(PST_EXPECTED, Integer.toString(expected));
                metadata.set(PST_EMITTED, Integer.toString(emitted[0]));
                metadata.set(PST_FAILED, Integer.toString(failed));
                if (failed > 0) {
                    logger.warn("PST email loss in \"{}\": expected {} messages, emitted {} ({} failed).",
                            pstPath, expected, emitted[0], failed);
                }
            } else {
                // Descriptor enumeration failed, so we have no ground-truth count to
                // reconcile against. Mark expected/failed as unmeasurable rather than
                // claiming zero loss, which would mask exactly the case we most want
                // to detect.
                metadata.set(PST_EXPECTED, "unknown");
                metadata.set(PST_EMITTED, Integer.toString(emitted[0]));
                metadata.set(PST_FAILED, "unknown");
                logger.warn("PST message count could not be measured for \"{}\"; emitted {} messages "
                        + "but loss is undetectable (descriptor enumeration failed).", pstPath, emitted[0]);
            }
        } catch (final TikaException e) {
            throw e;
        } catch (final Exception e) {
            throw new TikaException(e.getMessage(), e);
        } finally {
            if (pstFile != null && pstFile.getFileHandle() != null) {
                try {
                    pstFile.getFileHandle().close();
                } catch (final IOException e) {
                    // swallow closing exception
                }
            }
        }

        xhtml.endDocument();
    }

    private void walkFolder(final PSTFolder folder, final String folderPath, final XHTMLContentHandler xhtml,
                            final EmbeddedDocumentExtractor extractor, final Set<Long> emittedIds,
                            final int[] emitted) {
        try {
            if (folder.getContentCount() > 0) {
                PSTObject child = nextChild(folder, folderPath);
                while (child != null) {
                    if (child instanceof PSTMessage) {
                        emitMessage((PSTMessage) child, folderPath, xhtml, extractor, emittedIds, emitted);
                    }
                    child = nextChild(folder, folderPath);
                }
            }
        } catch (final Exception e) {
            logger.warn("PST folder \"{}\" content enumeration failed; skipping its messages.", folderPath, e);
        }

        try {
            if (folder.hasSubfolders()) {
                for (final PSTFolder sub : folder.getSubFolders()) {
                    String subName;
                    try {
                        subName = sub.getDisplayName();
                    } catch (final Exception e) {
                        subName = "?";
                    }
                    final String subPath = folderPath.endsWith("/") ? folderPath + subName : folderPath + "/" + subName;
                    walkFolder(sub, subPath, xhtml, extractor, emittedIds, emitted);
                }
            }
        } catch (final Exception e) {
            logger.warn("PST subfolder enumeration under \"{}\" failed; skipping its subfolders.", folderPath, e);
        }
    }

    private PSTObject nextChild(final PSTFolder folder, final String folderPath) {
        try {
            return folder.getNextChild();
        } catch (final Exception e) {
            logger.warn("PST folder \"{}\" getNextChild failed; stopping this folder early.", folderPath, e);
            return null;
        }
    }

    private void recoverOrphans(final List<Integer> ids, final XHTMLContentHandler xhtml,
                                final EmbeddedDocumentExtractor extractor, final Set<Long> emittedIds,
                                final int[] emitted, final PSTFile pstFile) {
        for (final int id : ids) {
            // Match getDescriptorNodeId()'s widening (sign-extend) so the dedup check below
            // agrees with the ids stored during the folder walk; a zero-extend mask here
            // would miss messages whose NID has bit 31 set and double-count them.
            final long descriptorId = id;
            if (emittedIds.contains(descriptorId)) {
                continue;
            }
            try {
                final PSTObject object = PSTObject.detectAndLoadPSTObject(pstFile, (long) id);
                if (object instanceof PSTMessage) {
                    emitMessage((PSTMessage) object, "/[recovered]", xhtml, extractor, emittedIds, emitted);
                }
            } catch (final Exception e) {
                logger.debug("PST orphan descriptor {} is not a loadable message; skipping.", id, e);
            }
        }
    }

    private void emitMessage(final PSTMessage message, final String folderPath, final XHTMLContentHandler xhtml,
                             final EmbeddedDocumentExtractor extractor, final Set<Long> emittedIds,
                             final int[] emitted) {
        final long descriptorId = message.getDescriptorNodeId();
        if (!emittedIds.add(descriptorId)) {
            return;
        }
        String subject = null;
        try {
            subject = message.getSubject();
        } catch (final Exception e) {
            // ignore: subject is best-effort for the resource name only
        }
        final Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE, PSTMailItemParser.PST_MAIL_ITEM_STRING);
        metadata.set(PST.PST_FOLDER_PATH, folderPath);
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, (subject == null ? "untitled" : subject) + ".msg");

        final long length = estimateSize(message);
        try (TikaInputStream tis = TikaInputStream.getFromContainer(message, length, metadata)) {
            extractor.parseEmbedded(tis, xhtml, metadata, true);
            emitted[0]++;
        } catch (final Exception e) {
            emittedIds.remove(descriptorId);
            logger.warn("Failed to emit PST message \"{}\" (descriptor {}) in folder \"{}\".",
                    subject, descriptorId, folderPath, e);
        }
    }

    // Mirrors OutlookPSTParser.estimateSize: a rough body-size estimate so the
    // embedded stream does not trip Tika's zip-bomb guard.
    private static long estimateSize(final PSTMessage message) {
        long size = 0;
        size += stringLength(safeBody(message));
        size += stringLength(safeBodyHTML(message));
        size += stringLength(safeSubject(message));
        size += 100_000;
        return size;
    }

    private static String safeBody(final PSTMessage message) {
        try {
            return message.getBody();
        } catch (final Exception e) {
            return null;
        }
    }

    private static String safeBodyHTML(final PSTMessage message) {
        try {
            return message.getBodyHTML();
        } catch (final Exception e) {
            return null;
        }
    }

    private static String safeSubject(final PSTMessage message) {
        try {
            return message.getSubject();
        } catch (final Exception e) {
            return null;
        }
    }

    private static long stringLength(final String value) {
        return value == null ? 0 : value.length();
    }
}
