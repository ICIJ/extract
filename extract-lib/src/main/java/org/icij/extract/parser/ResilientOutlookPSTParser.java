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

    // Reconciliation value used when the descriptor count can't be measured, so we
    // never claim "zero loss" for a PST whose ground truth we failed to read.
    private static final String UNKNOWN_COUNT = "unknown";
    // Folder path stamped on messages found only through descriptor recovery, i.e.
    // messages no visible folder links to.
    private static final String RECOVERED_FOLDER_PATH = "/[recovered]";

    @Override
    public Set<MediaType> getSupportedTypes(final ParseContext context) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
                      final ParseContext context) throws IOException, SAXException, TikaException {
        // Turn Tika's inputs into the collaborators the rest of the parse relies on.
        final EmbeddedDocumentExtractor extractor = EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(context);
        metadata.set(Metadata.CONTENT_TYPE, MS_OUTLOOK_PST_MIMETYPE.toString());
        final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        final EmissionContext emission = new EmissionContext(xhtml, extractor);

        xhtml.startDocument();
        final String pstPath = TikaInputStream.get(stream).getFile().getPath();

        PSTFile pstFile = null;
        try {
            pstFile = openPstFile(pstPath);
            extractAllMessages(pstFile, pstPath, emission, metadata);
        } catch (final TikaException e) {
            throw e;
        } catch (final Exception e) {
            throw new TikaException(e.getMessage(), e);
        } finally {
            closeQuietly(pstFile);
        }

        xhtml.endDocument();
    }

    // Opens the PST and rejects the one format java-libpst cannot read, so the rest
    // of the parse can assume a usable file.
    private PSTFile openPstFile(final String pstPath) throws Exception {
        final PSTFile pstFile = new PSTFile(pstPath);
        if (pstFile.getPSTFileType() == PSTFile.PST_TYPE_2013_UNICODE) {
            throw new TikaException("OST 2013 is not supported by java-libpst");
        }
        return pstFile;
    }

    // Emits every reachable message -- first through the visible folder hierarchy,
    // then through descriptor recovery for messages no folder links to -- and records
    // how the emitted count reconciles against the descriptor ground truth.
    private void extractAllMessages(final PSTFile pstFile, final String pstPath,
                                    final EmissionContext emission, final Metadata metadata) throws Exception {
        final List<Integer> messageDescriptorIds = enumerateMessageDescriptorIds(pstFile, pstPath);
        walkFolder(pstFile.getRootFolder(), "/", emission);
        if (messageDescriptorIds != null) {
            recoverOrphans(messageDescriptorIds, pstFile, emission);
        }
        recordReconciliation(metadata, pstPath, messageDescriptorIds, emission.emittedCount());
    }

    // Enumerates the descriptor ids of normal mail messages -- our ground-truth count
    // for loss detection. Returns null when enumeration itself fails, which the
    // reconciliation step reads as "loss is unmeasurable".
    private List<Integer> enumerateMessageDescriptorIds(final PSTFile pstFile, final String pstPath) {
        try {
            return PstMessageDescriptors.normalMessageDescriptorIds(pstFile);
        } catch (final Exception e) {
            logger.warn("Could not enumerate PST message descriptors for \"{}\"", pstPath, e);
            return null;
        }
    }

    // Walks one folder: emits the messages it directly holds, then recurses into each
    // subfolder. The two passes are isolated separately so a failure in one can never
    // abort the other or the rest of the tree.
    private void walkFolder(final PSTFolder folder, final String folderPath, final EmissionContext emission) {
        emitFolderMessages(folder, folderPath, emission);
        walkSubFolders(folder, folderPath, emission);
    }

    private void emitFolderMessages(final PSTFolder folder, final String folderPath, final EmissionContext emission) {
        try {
            if (folder.getContentCount() <= 0) {
                return;
            }
            PSTObject child = nextChild(folder, folderPath);
            while (child != null) {
                if (child instanceof PSTMessage) {
                    emitMessage((PSTMessage) child, folderPath, emission);
                }
                child = nextChild(folder, folderPath);
            }
        } catch (final Exception | LinkageError e) {
            logger.warn("PST folder \"{}\" content enumeration failed; skipping its messages.", folderPath, e);
        }
    }

    private void walkSubFolders(final PSTFolder folder, final String folderPath, final EmissionContext emission) {
        try {
            if (!folder.hasSubfolders()) {
                return;
            }
            for (final PSTFolder subFolder : folder.getSubFolders()) {
                walkFolder(subFolder, childFolderPath(folderPath, subFolder), emission);
            }
        } catch (final Exception | LinkageError e) {
            logger.warn("PST subfolder enumeration under \"{}\" failed; skipping its subfolders.", folderPath, e);
        }
    }

    // Builds a subfolder's display path, tolerating a name we cannot read so a corrupt
    // folder name never stops the walk.
    private String childFolderPath(final String parentPath, final PSTFolder subFolder) {
        final String subFolderName = safeDisplayName(subFolder);
        if (parentPath.endsWith("/")) {
            return parentPath + subFolderName;
        }
        return parentPath + "/" + subFolderName;
    }

    private String safeDisplayName(final PSTFolder folder) {
        final String displayName = safe(folder::getDisplayName);
        return displayName == null ? "?" : displayName;
    }

    // Reads the next child, turning a hard enumeration failure into a clean end of
    // folder so one bad entry can't abort the folder mid-iteration.
    private PSTObject nextChild(final PSTFolder folder, final String folderPath) {
        try {
            return folder.getNextChild();
        } catch (final Exception | LinkageError e) {
            logger.warn("PST folder \"{}\" getNextChild failed; stopping this folder early.", folderPath, e);
            return null;
        }
    }

    // Recovers messages that exist as descriptors but are linked into no visible
    // folder, so deleted-but-recoverable mail is not silently lost.
    private void recoverOrphans(final List<Integer> messageDescriptorIds, final PSTFile pstFile,
                                final EmissionContext emission) {
        for (final int descriptorNodeId : messageDescriptorIds) {
            recoverOrphan(descriptorNodeId, pstFile, emission);
        }
    }

    private void recoverOrphan(final int descriptorNodeId, final PSTFile pstFile, final EmissionContext emission) {
        // Match getDescriptorNodeId()'s widening (sign-extend) so this dedup check
        // agrees with the ids stored during the folder walk; a zero-extend mask here
        // would miss messages whose NID has bit 31 set and double-count them.
        final long descriptorId = descriptorNodeId;
        if (emission.alreadyEmitted(descriptorId)) {
            return;
        }
        try {
            final PSTObject object = PSTObject.detectAndLoadPSTObject(pstFile, descriptorId);
            if (object instanceof PSTMessage) {
                emitMessage((PSTMessage) object, RECOVERED_FOLDER_PATH, emission);
            }
        } catch (final Exception | LinkageError e) {
            logger.debug("PST orphan descriptor {} is not a loadable message; skipping.", descriptorNodeId, e);
        }
    }

    // Hands one message to Tika's PSTMailItemParser in isolation: a failure here is
    // logged and the descriptor un-marked so reconciliation still counts it as a loss,
    // but it never propagates to abort the surrounding walk.
    private void emitMessage(final PSTMessage message, final String folderPath, final EmissionContext emission) {
        final long descriptorId = message.getDescriptorNodeId();
        // Dedup: a message reachable through both its folder and orphan recovery must
        // be emitted exactly once.
        if (!emission.markEmitted(descriptorId)) {
            return;
        }
        // subject is best-effort: it only feeds the resource name and the failure log.
        final String subject = safe(message::getSubject);
        final Metadata metadata = buildMessageMetadata(folderPath, subject);

        final long estimatedSize = estimateSize(message);
        try (TikaInputStream messageStream = TikaInputStream.getFromContainer(message, estimatedSize, metadata)) {
            emission.parseEmbedded(messageStream, metadata);
            emission.incrementEmitted();
        } catch (final Exception | LinkageError e) {
            // A classpath/linkage failure (e.g. a dependency clash during attachment
            // detection) on one message must not abort the rest of the PST.
            emission.unmarkEmitted(descriptorId);
            logger.warn("Failed to emit PST message \"{}\" (descriptor {}) in folder \"{}\".",
                    subject, descriptorId, folderPath, e);
        }
    }

    // Builds the per-message metadata that routes the body to PSTMailItemParser and
    // gives it a folder path and a resource name.
    private Metadata buildMessageMetadata(final String folderPath, final String subject) {
        final Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE, PSTMailItemParser.PST_MAIL_ITEM_STRING);
        metadata.set(PST.PST_FOLDER_PATH, folderPath);
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, resourceName(subject));
        return metadata;
    }

    // Falls back to a stable name when the subject is missing, so every emitted message
    // has a usable .msg resource name.
    private static String resourceName(final String subject) {
        final String baseName = subject == null ? "untitled" : subject;
        return baseName + ".msg";
    }

    // Records expected/emitted/failed counts on the root document so any residual loss
    // is visible and alertable downstream.
    private void recordReconciliation(final Metadata metadata, final String pstPath,
                                      final List<Integer> messageDescriptorIds, final int emittedCount) {
        final boolean haveGroundTruth = messageDescriptorIds != null;
        if (haveGroundTruth) {
            recordMeasuredReconciliation(metadata, pstPath, messageDescriptorIds.size(), emittedCount);
        } else {
            recordUnmeasurableReconciliation(metadata, pstPath, emittedCount);
        }
    }

    private void recordMeasuredReconciliation(final Metadata metadata, final String pstPath,
                                              final int expected, final int emittedCount) {
        final int failed = Math.max(0, expected - emittedCount);
        metadata.set(PST_EXPECTED, Integer.toString(expected));
        metadata.set(PST_EMITTED, Integer.toString(emittedCount));
        metadata.set(PST_FAILED, Integer.toString(failed));
        if (failed > 0) {
            logger.warn("PST email loss in \"{}\": expected {} messages, emitted {} ({} failed).",
                    pstPath, expected, emittedCount, failed);
        }
    }

    // Descriptor enumeration failed, so we have no ground-truth count to reconcile
    // against. Mark expected/failed as unmeasurable rather than claiming zero loss,
    // which would mask exactly the case we most want to detect.
    private void recordUnmeasurableReconciliation(final Metadata metadata, final String pstPath,
                                                  final int emittedCount) {
        metadata.set(PST_EXPECTED, UNKNOWN_COUNT);
        metadata.set(PST_EMITTED, Integer.toString(emittedCount));
        metadata.set(PST_FAILED, UNKNOWN_COUNT);
        logger.warn("PST message count could not be measured for \"{}\"; emitted {} messages "
                + "but loss is undetectable (descriptor enumeration failed).", pstPath, emittedCount);
    }

    // Closes the underlying file handle, ignoring close-time failures: by the time we
    // get here the content is already emitted, so a failing close must not mask it.
    private static void closeQuietly(final PSTFile pstFile) {
        if (pstFile == null || pstFile.getFileHandle() == null) {
            return;
        }
        try {
            pstFile.getFileHandle().close();
        } catch (final IOException e) {
            // nothing actionable: the parse already produced its output
        }
    }

    // Mirrors OutlookPSTParser.estimateSize (Tika 3.3.0): a rough body-size estimate that
    // Tika's SecureContentHandler uses as the byte baseline for its zip-bomb guard (it
    // rejects a message once output characters exceed estimate * ratio), so a larger
    // estimate is protective against false rejections. We sum the same accessors Tika
    // does -- including getRTFBody(), which can dominate for RTF-bodied messages -- but
    // route each through safe(...): a corrupt message can throw here, and that must not
    // abort enumeration. If Tika changes which accessors it sums, follow it so the guard
    // stays calibrated the same way.
    private static long estimateSize(final PSTMessage message) {
        return 100_000
                + safeLength(message::getBody)
                + safeLength(message::getRTFBody)
                + safeLength(message::getBodyHTML)
                + safeLength(message::getSubject);
    }

    private static long safeLength(final StringSource source) {
        final String value = safe(source);
        return value == null ? 0 : value.length();
    }

    // Reads a value that a corrupt PST message may fail to produce, swallowing the failure
    // and returning null. Catches LinkageError as well as Exception to match the isolation
    // pattern used everywhere else in this class: a classpath/linkage failure on one
    // accessor must not abort enumeration of the rest of the PST.
    private static String safe(final StringSource source) {
        try {
            return source.get();
        } catch (final Exception | LinkageError e) {
            return null;
        }
    }

    // A message accessor that may throw on a corrupt message; lets safe(...) wrap any
    // libpst getter uniformly.
    @FunctionalInterface
    private interface StringSource {
        String get() throws Exception;
    }

    // Mutable per-parse state shared by the folder walk, the orphan-recovery pass, and
    // message emission: the output sink plus the dedup set and emitted counter. Carrying
    // it as one collaborator keeps it out of every traversal method's signature.
    private static final class EmissionContext {

        private final XHTMLContentHandler xhtml;
        private final EmbeddedDocumentExtractor extractor;
        private final Set<Long> emittedDescriptorIds = new HashSet<>();
        private int emittedCount = 0;

        private EmissionContext(final XHTMLContentHandler xhtml, final EmbeddedDocumentExtractor extractor) {
            this.xhtml = xhtml;
            this.extractor = extractor;
        }

        // Returns true the first time a descriptor is seen, false on a duplicate.
        private boolean markEmitted(final long descriptorId) {
            return emittedDescriptorIds.add(descriptorId);
        }

        // Rolls back a mark when emission failed, so the descriptor counts as a loss.
        private void unmarkEmitted(final long descriptorId) {
            emittedDescriptorIds.remove(descriptorId);
        }

        private boolean alreadyEmitted(final long descriptorId) {
            return emittedDescriptorIds.contains(descriptorId);
        }

        private void parseEmbedded(final TikaInputStream messageStream, final Metadata metadata)
                throws SAXException, IOException {
            extractor.parseEmbedded(messageStream, xhtml, metadata, true);
        }

        private void incrementEmitted() {
            emittedCount++;
        }

        private int emittedCount() {
            return emittedCount;
        }
    }
}
