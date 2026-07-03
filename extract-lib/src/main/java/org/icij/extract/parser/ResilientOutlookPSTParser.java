package org.icij.extract.parser;

import com.pff.OstCompressedBlockReader;
import com.pff.PSTAttachment;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import com.pff.PSTNodeInputStream;
import com.pff.PSTObject;
import com.pff.PstFolderPathResolver;
import com.pff.PstMessageDescriptors;
import org.apache.tika.exception.EncryptedDocumentException;
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

import org.icij.extract.extractor.EmbedSpawner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
 *
 * <p><b>Encrypted-attachment residual.</b> An attachment whose bytes are recovered
 * byte-perfectly but are password-protected is indexed as an attachment, yet its body
 * text cannot be extracted without credentials -- a distinct residual class from a
 * reader defect, counted by {@link #PST_ATTACHMENTS_ENCRYPTED}. Note that under Tika's
 * default {@code ParsingEmbeddedDocumentExtractor} this counter reads 0 because that
 * extractor swallows {@code EncryptedDocumentException} internally; the recovered bytes
 * are emitted and indexed regardless. A password/credential extraction path is future
 * work, not handled here.
 */
public class ResilientOutlookPSTParser implements Parser {

    public static final MediaType MS_OUTLOOK_PST_MIMETYPE = MediaType.application("vnd.ms-outlook-pst");
    public static final String PST_EXPECTED = "tika:pst_expected";
    public static final String PST_EMITTED = "tika:pst_emitted";
    public static final String PST_FAILED = "tika:pst_failed";
    // Count of by-value attachments whose data java-libpst could not fully read. Set only
    // for OST 2013 (type-36) files, the one format where the defect occurs (see
    // countUnreadableAttachments); absent on every other PST.
    public static final String PST_ATTACHMENTS_UNREADABLE = "tika:pst_attachments_unreadable";
    // Count of unreadable by-value attachments whose bytes were recovered by the in-JVM zlib block
    // reader and re-emitted to the index. Set only for OST 2013 (type-36) files, alongside
    // PST_ATTACHMENTS_UNREADABLE.
    public static final String PST_ATTACHMENTS_RECOVERED = "tika:pst_attachments_recovered";
    // Count of unreadable by-value attachments the reader could not recover (failed the size gate or
    // could not be resolved). The honest residual loss: PST_ATTACHMENTS_UNREADABLE minus recovered.
    public static final String PST_ATTACHMENTS_UNRECOVERED = "tika:pst_attachments_unrecovered";
    // Count of recovered attachments whose bytes are password-protected, so the bytes are indexed but
    // body text cannot be extracted. A subset of PST_ATTACHMENTS_RECOVERED.
    // Note: with Tika's default ParsingEmbeddedDocumentExtractor this reads "0" because that extractor swallows EncryptedDocumentException internally; the recovered bytes are still emitted and indexed regardless.
    public static final String PST_ATTACHMENTS_ENCRYPTED = "tika:pst_attachments_encrypted";
    // Per-attachment recovery marker promoted by datashare into the typed Document.recoveryStatus
    // field. Value is one of RECOVERED | ENCRYPTED | UNRECOVERED.
    public static final String PST_ATTACHMENT_RECOVERY = "tika:pst_attachment_recovery";
    // Stable, cross-run resumable key stamped on every emitted message: the message's descriptor
    // node id. Datashare persists it once the message subtree is durably indexed and supplies a
    // ResumePolicy that skips it on a later resumed run. Not read by DigestIdentifier, so stamping
    // it never perturbs embed identity (see the resumable-OST design doc).
    public static final String PST_RESUME_KEY = "tika:pst_resume_key";
    // Count of messages skipped on this run because a ResumePolicy reported them already emitted and
    // durably indexed by a previous run. Set on the root only when > 0, so a non-resumed run never
    // carries a misleading "0".
    public static final String PST_RESUMED_SKIPPED = "tika:pst_resumed_skipped";

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
        final Reconciliation reconciliation = new Reconciliation();
        // Resume skip policy (default: skip nothing). Consulted per message in emitMessage.
        final ResumePolicy resumePolicy =
                context.get(ResumePolicy.class) != null ? context.get(ResumePolicy.class) : ResumePolicy.NONE;
        final EmissionContext emission = new EmissionContext(xhtml, extractor, reconciliation, resumePolicy);
        final PstFanoutConfig fanout = context.get(PstFanoutConfig.class);

        xhtml.startDocument();
        final String pstPath = TikaInputStream.get(stream).getFile().getPath();

        PstStdoutFilter.install();
        PstStdoutFilter.begin();
        PSTFile pstFile = null;
        try {
            pstFile = new PSTFile(pstPath);
            extractAllMessages(pstFile, pstPath, emission, metadata, reconciliation, fanout);
        } catch (final TikaException e) {
            throw e;
        } catch (final Exception e) {
            throw new TikaException(e.getMessage(), e);
        } finally {
            closeQuietly(pstFile);
            PstStdoutFilter.end();
        }

        xhtml.endDocument();
    }

    // Emits every reachable message -- first through the visible folder hierarchy,
    // then through descriptor recovery for messages no folder links to -- and records
    // how the emitted count reconciles against the descriptor ground truth.
    private void extractAllMessages(final PSTFile pstFile, final String pstPath,
                                    final EmissionContext emission, final Metadata metadata,
                                    final Reconciliation reconciliation,
                                    final PstFanoutConfig fanout) throws Exception {
        // java-libpst reads single-block data from OST 2013 (type-36) files correctly, but
        // truncates or fails on attachment data spanning multiple blocks. We can't repair the
        // bytes, so flag the affected attachments to make the loss visible. Confined to type-36
        // because that is the only format with the defect; normal PSTs pay nothing.
        if (pstFile.getPSTFileType() == PSTFile.PST_TYPE_2013_UNICODE) {
            emission.enableAttachmentIntegrityCheck();
        }
        final List<Integer> messageDescriptorIds = enumerateMessageDescriptorIds(pstFile, pstPath);
        final boolean fanoutRequested = fanout != null && fanout.enabled()
                && emission.extractor instanceof EmbedSpawner;
        final Map<Integer, String> folderPaths =
                (fanoutRequested || messageDescriptorIds != null) ? resolveFolderPaths(pstFile, pstPath) : null;

        final boolean canFanOut = fanoutRequested && folderPaths != null && folderPaths.size() > 1;

        if (canFanOut) {
            walkFoldersParallel(pstPath, folderPaths, emission, reconciliation, fanout.executor().get());
        } else {
            walkFolder(pstFile.getRootFolder(), "/", emission);
        }

        if (messageDescriptorIds != null) {
            // folderPaths is non-null here: the line-161 guard resolves it whenever messageDescriptorIds
            // is non-null, and resolveFolderPaths never returns null (empty map on failure).
            recoverOrphans(messageDescriptorIds, pstFile, folderPaths, emission);
        }
        // The java-libpst-heavy work is done; lift suppression only for the duration of the
        // per-PST reconciliation and attachment-integrity summary (the load-bearing signal) so it
        // reaches the console too, not only the FILE appender. Lifting (rather than ending the parse
        // early) keeps the depth counter balanced, so a nested PST-in-PST parse stays suppressed
        // until the outermost parse's finally calls end().
        PstStdoutFilter.runWithSuppressionLifted(() -> {
            recordReconciliation(metadata, pstPath, messageDescriptorIds, emission.emittedCount());
            recordAttachmentIntegrity(metadata, pstPath, emission);
            recordResumeProgress(metadata, pstPath, emission);
        });
    }

    // One task per folder. Each task opens its OWN PSTFile handle (libpst is not thread-safe),
    // loads its folder by descriptor id, and emits that folder's DIRECT messages through a FORKED
    // EmbedSpawner (its own DFS stack). The shared Reconciliation dedups + counts across tasks.
    //
    // Handle ownership is PER TASK: each task opens its handle at the start of its body and closes it in
    // its OWN finally, so a handle is only ever closed by the same thread that read it, after that read
    // completes or aborts. This makes a close-during-read race impossible by construction, even on the
    // abnormal path: the controller's finally cancels (interrupts) still-running tasks, and each task
    // closes its own handle before the pool thread returns.
    //
    // TRADEOFF: concurrent open-handle count is still bounded by the pool size (each thread holds one
    // open handle at a time, closed before it picks up the next task), but a handle is opened per folder
    // task rather than reused per thread. This is the correctness-over-reuse choice; the OS page cache
    // serves the repeated header reads cheaply. A per-thread handle cache with a proper task-completion
    // latch is a possible future optimization.
    private void walkFoldersParallel(final String pstPath, final Map<Integer, String> folderPaths,
                                     final EmissionContext baseEmission, final Reconciliation reconciliation,
                                     final ExecutorService executor) throws Exception {
        List<Future<?>> futures = Collections.emptyList();
        try {
            futures = submitFolderTasks(pstPath, folderPaths, baseEmission, reconciliation, executor);
            awaitAllTasks(futures);
        } finally {
            // On the abnormal path, cancel interrupts still-running tasks; each closes its OWN handle in
            // its own finally. The controller never touches any handle.
            cancelFutures(futures);
        }
    }

    private List<Future<?>> submitFolderTasks(final String pstPath, final Map<Integer, String> folderPaths,
                                              final EmissionContext baseEmission, final Reconciliation reconciliation,
                                              final ExecutorService executor) {
        final List<Future<?>> futures = new ArrayList<>(folderPaths.size());
        for (final Map.Entry<Integer, String> entry : folderPaths.entrySet()) {
            final int descriptorId = entry.getKey();
            final String folderPath = entry.getValue();
            futures.add(executor.submit(() -> walkSingleFolderParallel(
                    pstPath, descriptorId, folderPath, baseEmission, reconciliation
            )));
        }
        return futures;
    }

    private void walkSingleFolderParallel(final String pstPath, final int descriptorId,
                                          final String folderPath, final EmissionContext baseEmission,
                                          final Reconciliation reconciliation) {
        PstStdoutFilter.runWithSuppression(() -> {
            // Open this task's OWN handle; the task body then owns closing it (see runOwnedFolderTask),
            // so it is never closed by another thread while this thread is still reading it.
            final PSTFile own = openQuietly(pstPath);
            if (own == null) {
                return;
            }
            runOwnedFolderTask(own, descriptorId, folderPath, baseEmission, reconciliation);
        });
    }

    // Runs one folder task against a handle the CALLER has opened and this task now OWNS: the detect+emit
    // is wrapped so any failure is logged, and the handle is closed in this task's OWN finally, on this
    // task's OWN thread, whether the body returns normally or throws. Package-private so a test can inject
    // a handle whose close() is observable and assert the finally closes it exactly once even on throw.
    void runOwnedFolderTask(final PSTFile own, final int descriptorId, final String folderPath,
                            final EmissionContext baseEmission, final Reconciliation reconciliation) {
        try {
            walkOneFolder(own, descriptorId, folderPath, baseEmission, reconciliation);
        } catch (final Exception | LinkageError e) {
            logger.warn("PST folder \"{}\" (descriptor {}) parallel walk failed; skipping.",
                    folderPath, descriptorId, e);
        } finally {
            closeQuietly(own);
        }
    }

    // Detect + emit for a single folder against the GIVEN handle. Package-private so a test can drive the
    // detect+emit against an injected handle and assert the caller's finally closes it exactly once.
    // The caller (walkSingleFolderParallel) owns opening and closing `handle`.
    void walkOneFolder(final PSTFile handle, final int descriptorId, final String folderPath,
                       final EmissionContext baseEmission, final Reconciliation reconciliation) throws Exception {
        final PSTObject object = PSTObject.detectAndLoadPSTObject(handle, (long) descriptorId);
        if (object instanceof PSTFolder) {
            emitFolderParallel((PSTFolder) object, folderPath, baseEmission, reconciliation);
        }
    }

    private void emitFolderParallel(final PSTFolder folder, final String folderPath,
                                    final EmissionContext baseEmission, final Reconciliation reconciliation) {
        final EmbedSpawner baseSpawner = (EmbedSpawner) baseEmission.extractor;
        final EmissionContext walkerEmission =
                new EmissionContext(baseEmission.xhtml, baseSpawner.fork(), reconciliation, baseEmission.resumePolicy);
        emitFolderMessages(folder, folderPath, walkerEmission);
    }

    private void awaitAllTasks(final List<Future<?>> futures) {
        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return; // stop waiting; walkFoldersParallel (finally) cancels the rest
            } catch (final ExecutionException e) {
                logger.warn("PST folder task failed.", e.getCause());
            }
        }
    }

    private void cancelFutures(final List<Future<?>> futures) {
        for (final Future<?> f : futures) {
            f.cancel(true);
        }
    }

    private static PSTFile openQuietly(final String pstPath) {
        try {
            return new PSTFile(pstPath);
        } catch (final Exception | LinkageError e) {
            LoggerFactory.getLogger(ResilientOutlookPSTParser.class)
                    .warn("Could not open a per-walker PSTFile handle for \"{}\".", pstPath, e);
            return null;
        }
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

    // Builds the descriptor-tree folder-path map used to attribute orphan-recovered messages.
    // Returns an empty map on failure so recovery degrades to the /[recovered] sentinel rather
    // than aborting -- attribution is best-effort, never load-bearing for emission.
    private Map<Integer, String> resolveFolderPaths(final PSTFile pstFile, final String pstPath) {
        try {
            return PstFolderPathResolver.folderPaths(pstFile);
        } catch (final Exception | LinkageError e) {
            logger.warn("Could not resolve PST folder paths for \"{}\"; recovered messages will use {}.",
                    pstPath, RECOVERED_FOLDER_PATH, e);
            return Collections.emptyMap();
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
        final String displayName = safe(folder::getDisplayName, null);
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
                                final Map<Integer, String> folderPaths, final EmissionContext emission) {
        for (final int descriptorNodeId : messageDescriptorIds) {
            recoverOrphan(descriptorNodeId, pstFile, folderPaths, emission);
        }
    }

    private void recoverOrphan(final int descriptorNodeId, final PSTFile pstFile,
                               final Map<Integer, String> folderPaths, final EmissionContext emission) {
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
                final int parentId = object.getDescriptorNode().parentDescriptorIndexIdentifier;
                emitMessage((PSTMessage) object, folderPathOrRecovered(folderPaths, parentId), emission);
            }
        } catch (final Exception | LinkageError e) {
            logger.debug("PST orphan descriptor {} is not a loadable message; skipping.", descriptorNodeId, e);
        }
    }

    // Maps a recovered message's parent folder descriptor id to its real folder path, falling
    // back to the recovered sentinel when the parent is not a resolvable folder.
    static String folderPathOrRecovered(final Map<Integer, String> folderPaths, final int parentId) {
        return folderPaths.getOrDefault(parentId, RECOVERED_FOLDER_PATH);
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
        // Resume: a unit a previous run already emitted and durably indexed is skipped WITHOUT
        // re-parsing. It stays marked emitted (so orphan recovery does not re-reach it) and counts
        // toward the emitted total (so reconciliation does not read it as loss); only the parse/emit
        // work is elided. Inert unless a ResumePolicy is supplied (default resumes nothing), so a
        // non-resumed run behaves exactly as before.
        if (emission.isUnitDone(descriptorId)) {
            emission.incrementEmitted();
            emission.incrementResumeSkipped();
            return;
        }
        // subject is best-effort: it only feeds the resource name and the failure log.
        final String subject = safe(message::getSubject, null);
        final Metadata metadata = buildMessageMetadata(folderPath, subject, descriptorId);

        final long estimatedSize = estimateSize(message);
        try (TikaInputStream messageStream = TikaInputStream.getFromContainer(message, estimatedSize, metadata)) {
            emission.parseEmbedded(messageStream, metadata);
            emission.incrementEmitted();
            // Scan attachments only for a successfully emitted message, and only here -- inside the
            // dedup guard. A failed emission un-marks the descriptor (counting it as a loss), which
            // lets orphan recovery re-reach it; scanning before that point would double-count the
            // attachments of any message reached through both the folder walk and orphan recovery.
            if (emission.shouldCheckAttachmentIntegrity()) {
                countUnreadableAttachments(message, folderPath, emission);
            }
        } catch (final Exception | LinkageError e) {
            // A classpath/linkage failure (e.g. a dependency clash during attachment
            // detection) on one message must not abort the rest of the PST.
            emission.unmarkEmitted(descriptorId);
            logger.warn("Failed to emit PST message \"{}\" (descriptor {}) in folder \"{}\".",
                    subject, descriptorId, folderPath, e);
        }
    }

    // Best-effort: count by-value attachments whose data can't be fully read, so the OST 2013
    // multi-block truncation surfaces as an honest data-completeness signal instead of only as
    // misleading "corrupt PDF" / "premature end of JPEG" errors deeper in the pipeline. Never
    // throws: a failure to inspect one attachment must not abort the surrounding walk.
    private void countUnreadableAttachments(final PSTMessage message, final String folderPath,
                                            final EmissionContext emission) {
        final int attachmentCount;
        try {
            attachmentCount = message.getNumberOfAttachments();
        } catch (final Exception | LinkageError e) {
            return;
        }
        for (int index = 0; index < attachmentCount; index++) {
            final PSTAttachment attachment = loadAttachment(message, index);
            if (attachment != null && isUnreadableByValueAttachment(attachment, folderPath)) {
                emission.incrementUnreadableAttachments();
                recoverAndEmitAttachment(attachment, message, index, folderPath, emission);
            }
        }
    }

    // Loads one attachment, isolating a libpst failure so a single bad attachment never aborts the
    // scan. Returns null when the attachment can't be loaded.
    private PSTAttachment loadAttachment(final PSTMessage message, final int index) {
        try {
            return message.getAttachment(index);
        } catch (final Exception | LinkageError e) {
            return null;
        }
    }

    // Fallback: java-libpst could not read this by-value attachment, but the in-JVM zlib block reader
    // can. Recover its bytes and emit them as an embedded document so the content still reaches the
    // index. Fully isolated and best-effort: when recovery fails the attachment stays counted as an
    // unrecovered loss. Gated by the same OST-2013 check as the detection above.
    // The attachment is pre-loaded by the caller (countUnreadableAttachments) to avoid a redundant load.
    private void recoverAndEmitAttachment(final PSTAttachment attachment, final PSTMessage message,
                                          final int index, final String folderPath,
                                          final EmissionContext emission) {
        final String name = safeAttachmentName(attachment);
        final String relationshipId = relationshipId(message, index);
        final Optional<byte[]> recovered = OstCompressedBlockReader.recover(attachment);
        if (recovered.isEmpty()) {
            emission.incrementUnrecoveredAttachments();
            emitRecoveryStub(name, folderPath, relationshipId, "UNRECOVERED", emission);
            return;
        }
        final byte[] bytes = recovered.get();
        final Metadata attachmentMetadata = new Metadata();
        attachmentMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        attachmentMetadata.set(TikaCoreProperties.EMBEDDED_RELATIONSHIP_ID, relationshipId);
        attachmentMetadata.set(PST.PST_FOLDER_PATH, folderPath);
        attachmentMetadata.set(PST_ATTACHMENT_RECOVERY, "RECOVERED");
        try (final TikaInputStream stream = TikaInputStream.get(bytes, attachmentMetadata)) {
            emission.parseEmbedded(stream, attachmentMetadata);
            emission.incrementRecoveredAttachments();
            logger.info("Recovered attachment \"{}\" ({} bytes) in folder \"{}\" via the in-JVM zlib block reader.",
                    name, bytes.length, folderPath);
        } catch (final EncryptedDocumentException e) {
            emission.incrementRecoveredAttachments();
            emission.incrementEncryptedAttachments();
            emitRecoveryStub(name, folderPath, relationshipId, "ENCRYPTED", emission);
            logger.info("Recovered attachment \"{}\" in folder \"{}\" is encrypted; emitted as ENCRYPTED stub.",
                    name, folderPath);
        } catch (final Exception | LinkageError e) {
            emission.incrementUnrecoveredAttachments();
            emitRecoveryStub(name, folderPath, relationshipId, "UNRECOVERED", emission);
            logger.warn("Recovered attachment \"{}\" in folder \"{}\" could not be emitted; recorded as UNRECOVERED.",
                    name, folderPath, e);
        }
    }

    // Stable, unique relationship id for an attachment: its message descriptor id plus the
    // attachment index. Drives DigestIdentifier.generateForEmbed so stubs get deterministic,
    // collision-free ids even with empty content.
    private static String relationshipId(final PSTMessage message, final int index) {
        long descriptorId;
        try {
            descriptorId = message.getDescriptorNodeId();
        } catch (final Exception | LinkageError e) {
            descriptorId = -1L;
        }
        return formatRelationshipId(descriptorId, index);
    }

    // Pure formatter, package-private for unit testing (no PSTMessage needed).
    static String formatRelationshipId(final long descriptorId, final int index) {
        return descriptorId + "-" + index;
    }

    // Emits a content-less embedded document recording an attachment whose bytes could not be
    // indexed (UNRECOVERED) or could not be parsed without credentials (ENCRYPTED), so the loss is
    // visible and queryable per document instead of only as an aggregate parent counter.
    private void emitRecoveryStub(final String name, final String folderPath, final String relationshipId,
                                  final String status, final EmissionContext emission) {
        final Metadata stub = new Metadata();
        stub.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        stub.set(TikaCoreProperties.EMBEDDED_RELATIONSHIP_ID, relationshipId);
        stub.set(PST.PST_FOLDER_PATH, folderPath);
        stub.set(PST_ATTACHMENT_RECOVERY, status);
        try (final TikaInputStream stream = TikaInputStream.get(new byte[0], stub)) {
            emission.parseEmbedded(stream, stub);
        } catch (final Exception | LinkageError e) {
            logger.warn("Could not emit {} recovery stub for attachment \"{}\" in folder \"{}\".",
                    status, name, folderPath, e);
        }
    }

    // A by-value attachment is unreadable when its stream won't open, or reports fewer bytes than
    // its declared size (the type-36 multi-block truncation). Non-by-value attachments (embedded
    // messages, OLE, by-reference) carry no inline binary stream to check and are skipped. Returns
    // false on any inspection error so a libpst quirk never inflates the count.
    // The attachment is pre-loaded by the caller (countUnreadableAttachments) to avoid a redundant load.
    private boolean isUnreadableByValueAttachment(final PSTAttachment attachment,
                                                  final String folderPath) {
        if (safe(attachment::getAttachMethod, PSTAttachment.ATTACHMENT_METHOD_NONE) != PSTAttachment.ATTACHMENT_METHOD_BY_VALUE) {
            return false;
        }
        final long declaredSize = safe(attachment::getFilesize, -1);
        // Opening the stream re-reads the attachment's first block, and PSTMailItemParser reads it
        // again during the actual emit, so this duplicates a little I/O per by-value attachment.
        // That cost is accepted: opening is the only way to catch the "block won't decompress" and
        // mid-read failures, and the whole check is gated to OST 2013 files where the defect lives.
        try (InputStream stream = attachment.getFileInputStream()) {
            if (declaredSize > 0 && stream instanceof PSTNodeInputStream node) {
                final long readableSize = node.length();
                if (readableSize < declaredSize) {
                    logger.warn("PST attachment \"{}\" in folder \"{}\" is truncated: {} of {} declared bytes readable.",
                            safeAttachmentName(attachment), folderPath, readableSize, declaredSize);
                    return true;
                }
            }
            return false;
        } catch (final Exception | LinkageError e) {
            logger.warn("PST attachment \"{}\" in folder \"{}\" could not be read ({}).",
                    safeAttachmentName(attachment), folderPath, e.toString());
            return true;
        }
    }

    private static String safeAttachmentName(final PSTAttachment attachment) {
        final String longName = safe(attachment::getLongFilename, null);
        if (longName != null && !longName.isEmpty()) {
            return longName;
        }
        final String name = safe(attachment::getFilename, null);
        return name == null ? "(unnamed)" : name;
    }

    // Builds the per-message metadata that routes the body to PSTMailItemParser and
    // gives it a folder path and a resource name.
    private Metadata buildMessageMetadata(final String folderPath, final String subject, final long descriptorId) {
        final Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE, PSTMailItemParser.PST_MAIL_ITEM_STRING);
        metadata.set(PST.PST_FOLDER_PATH, folderPath);
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, resourceName(subject));
        // Stable, cross-run resumable key: datashare persists it once this message's subtree is
        // durably indexed, then supplies a ResumePolicy that skips it on a later resumed run. Not
        // read by DigestIdentifier (which uses only the content hash, parent id, relationship id and
        // resource name), so stamping it never perturbs this message's or its attachments' ids.
        metadata.set(PST_RESUME_KEY, Long.toString(descriptorId));
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
        } else if (emittedCount > expected) {
            // Not loss, but the ground-truth baseline was exceeded (orphan recovery surfaced messages
            // outside the enumerated folder tree, or the descriptor count under-enumerated). Surface it
            // rather than letting the clamped failed=0 read as a clean, fully-reconciled extraction.
            logger.warn("PST count mismatch in \"{}\": emitted {} messages, more than the {} enumerated "
                    + "descriptors; loss accounting may be incomplete.", pstPath, emittedCount, expected);
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

    // Records, for OST 2013 files only, how many by-value attachments could not be fully read.
    // The field is left absent on every other PST so it reads as "not applicable" rather than a
    // possibly-misleading "0" for a format we never scanned.
    private void recordAttachmentIntegrity(final Metadata metadata, final String pstPath,
                                           final EmissionContext emission) {
        if (!emission.shouldCheckAttachmentIntegrity()) {
            return;
        }
        final int unreadable = emission.unreadableAttachments();
        final int recovered = emission.recoveredAttachments();
        final int unrecovered = emission.unrecoveredAttachments();
        final int encrypted = emission.encryptedAttachments();
        metadata.set(PST_ATTACHMENTS_UNREADABLE, Integer.toString(unreadable));
        metadata.set(PST_ATTACHMENTS_RECOVERED, Integer.toString(recovered));
        metadata.set(PST_ATTACHMENTS_UNRECOVERED, Integer.toString(unrecovered));
        metadata.set(PST_ATTACHMENTS_ENCRYPTED, Integer.toString(encrypted));
        if (unreadable > 0) {
            logger.warn("PST \"{}\": {} by-value attachment(s) unreadable by java-libpst (OST 2013 multi-block "
                    + "limitation); {} recovered via the in-JVM zlib block reader, {} unrecovered, {} encrypted.",
                    pstPath, unreadable, recovered, unrecovered, encrypted);
        }
    }

    // Records, only on a resumed run that actually skipped something, how many messages were skipped
    // because a previous run had already emitted and durably indexed them. Left absent otherwise so a
    // non-resumed run never carries a misleading "0".
    private void recordResumeProgress(final Metadata metadata, final String pstPath,
                                      final EmissionContext emission) {
        final int skipped = emission.resumeSkipped();
        if (skipped > 0) {
            metadata.set(PST_RESUMED_SKIPPED, Integer.toString(skipped));
            logger.info("PST resume for \"{}\": skipped {} message(s) already emitted by a previous run.",
                    pstPath, skipped);
        }
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

    private static long safeLength(final ThrowingSupplier<String> source) {
        final String value = safe(source, null);
        return value == null ? 0 : value.length();
    }

    // Reads a value that a corrupt PST may fail to produce, swallowing the failure and returning
    // the fallback. Catches LinkageError as well as Exception to match the isolation pattern used
    // everywhere else in this class: a classpath/linkage failure on one accessor must not abort
    // enumeration of the rest of the PST.
    private static <T> T safe(final ThrowingSupplier<T> source, final T fallback) {
        try {
            return source.get();
        } catch (final Exception | LinkageError e) {
            return fallback;
        }
    }

    // A libpst accessor that may throw on a corrupt message or attachment; lets safe(...) wrap any
    // getter uniformly, whatever its return type.
    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    // Shared, thread-safe across all folder-walk tasks of one parse: the dedup set, the emitted
    // counter, and the OST-2013 attachment-integrity counters.
    private static final class Reconciliation {
        private final Set<Long> emittedDescriptorIds = ConcurrentHashMap.newKeySet();
        private final AtomicInteger emittedCount = new AtomicInteger();
        private volatile boolean checkAttachmentIntegrity = false;
        private final AtomicInteger unreadableAttachments = new AtomicInteger();
        private final AtomicInteger recoveredAttachments = new AtomicInteger();
        private final AtomicInteger unrecoveredAttachments = new AtomicInteger();
        private final AtomicInteger encryptedAttachments = new AtomicInteger();
        private final AtomicInteger resumeSkipped = new AtomicInteger();

        boolean markEmitted(final long id) { return emittedDescriptorIds.add(id); }
        void unmarkEmitted(final long id) { emittedDescriptorIds.remove(id); }
        boolean alreadyEmitted(final long id) { return emittedDescriptorIds.contains(id); }
        void incrementEmitted() { emittedCount.incrementAndGet(); }
        int emittedCount() { return emittedCount.get(); }
        void enableAttachmentIntegrityCheck() { checkAttachmentIntegrity = true; }
        boolean shouldCheckAttachmentIntegrity() { return checkAttachmentIntegrity; }
        void incrementUnreadableAttachments() { unreadableAttachments.incrementAndGet(); }
        int unreadableAttachments() { return unreadableAttachments.get(); }
        void incrementRecoveredAttachments() { recoveredAttachments.incrementAndGet(); }
        int recoveredAttachments() { return recoveredAttachments.get(); }
        void incrementUnrecoveredAttachments() { unrecoveredAttachments.incrementAndGet(); }
        int unrecoveredAttachments() { return unrecoveredAttachments.get(); }
        void incrementEncryptedAttachments() { encryptedAttachments.incrementAndGet(); }
        int encryptedAttachments() { return encryptedAttachments.get(); }
        void incrementResumeSkipped() { resumeSkipped.incrementAndGet(); }
        int resumeSkipped() { return resumeSkipped.get(); }
    }

    // Mutable per-parse state shared by the folder walk, the orphan-recovery pass, and
    // message emission: the output sink plus a reference to the shared Reconciliation.
    // Carrying it as one collaborator keeps it out of every traversal method's signature.
    private static final class EmissionContext {
        private final XHTMLContentHandler xhtml;
        private final EmbeddedDocumentExtractor extractor;
        private final Reconciliation reconciliation;
        // Read-only, safe to share across fan-out forks: skip predicate for resumed runs.
        private final ResumePolicy resumePolicy;

        private EmissionContext(final XHTMLContentHandler xhtml, final EmbeddedDocumentExtractor extractor,
                                final Reconciliation reconciliation, final ResumePolicy resumePolicy) {
            this.xhtml = xhtml;
            this.extractor = extractor;
            this.reconciliation = reconciliation;
            this.resumePolicy = resumePolicy;
        }

        private boolean isUnitDone(final long resumeKey) { return resumePolicy.isUnitDone(resumeKey); }
        private void incrementResumeSkipped() { reconciliation.incrementResumeSkipped(); }
        private int resumeSkipped() { return reconciliation.resumeSkipped(); }

        private void enableAttachmentIntegrityCheck() { reconciliation.enableAttachmentIntegrityCheck(); }
        private boolean shouldCheckAttachmentIntegrity() { return reconciliation.shouldCheckAttachmentIntegrity(); }
        private void incrementUnreadableAttachments() { reconciliation.incrementUnreadableAttachments(); }
        private int unreadableAttachments() { return reconciliation.unreadableAttachments(); }
        private void incrementRecoveredAttachments() { reconciliation.incrementRecoveredAttachments(); }
        private int recoveredAttachments() { return reconciliation.recoveredAttachments(); }
        private void incrementUnrecoveredAttachments() { reconciliation.incrementUnrecoveredAttachments(); }
        private int unrecoveredAttachments() { return reconciliation.unrecoveredAttachments(); }
        private void incrementEncryptedAttachments() { reconciliation.incrementEncryptedAttachments(); }
        private int encryptedAttachments() { return reconciliation.encryptedAttachments(); }
        private boolean markEmitted(final long id) { return reconciliation.markEmitted(id); }
        private void unmarkEmitted(final long id) { reconciliation.unmarkEmitted(id); }
        private boolean alreadyEmitted(final long id) { return reconciliation.alreadyEmitted(id); }
        private void incrementEmitted() { reconciliation.incrementEmitted(); }
        private int emittedCount() { return reconciliation.emittedCount(); }

        private void parseEmbedded(final TikaInputStream messageStream, final Metadata metadata)
                throws SAXException, IOException, TikaException {
            extractor.parseEmbedded(messageStream, xhtml, metadata, true);
        }
    }
}
