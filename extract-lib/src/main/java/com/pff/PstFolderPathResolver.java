package com.pff;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Helper placed in package {@code com.pff} so it can read java-libpst's
 * package-private descriptor tree. Computes each folder's full display path
 * ("/", "/Inbox", "/Inbox/Sub") from the descriptor tree's public parent links,
 * producing strings identical to {@code ResilientOutlookPSTParser}'s folder walk.
 *
 * <p>This lets orphan recovery stamp a recovered message with its true folder
 * path instead of the "/[recovered]" sentinel, without touching java-libpst's
 * broken stateful {@code PSTFolder.getNextChild()}.
 */
public final class PstFolderPathResolver {

    // Matches ResilientOutlookPSTParser.safeDisplayName: an unreadable folder name becomes "?".
    private static final String UNREADABLE_NAME = "?";

    private PstFolderPathResolver() {
    }

    public static Map<Integer, String> folderPaths(final PSTFile pst) throws IOException, PSTException {
        final int rootId = (int) pst.getRootFolder().getDescriptorNodeId();

        // Collect every folder descriptor's parent link (from the node, no load needed) and
        // its display name (needs a load, isolated so one bad folder degrades to "?").
        final Map<Integer, Integer> parentOf = new HashMap<>();
        final Map<Integer, String> nameOf = new HashMap<>();
        for (final LinkedList<DescriptorIndexNode> nodes : pst.getChildDescriptorTree().values()) {
            for (final DescriptorIndexNode node : nodes) {
                if (PSTObject.getNodeType(node.descriptorIdentifier) != PSTObject.NID_TYPE_NORMAL_FOLDER) {
                    continue;
                }
                parentOf.put(node.descriptorIdentifier, node.parentDescriptorIndexIdentifier);
                nameOf.put(node.descriptorIdentifier, displayName(pst, node.descriptorIdentifier));
            }
        }

        final Map<Integer, String> paths = new HashMap<>();
        for (final Integer folderId : parentOf.keySet()) {
            paths.put(folderId, buildPath(folderId, rootId, parentOf, nameOf));
        }
        paths.put(rootId, "/");
        return paths;
    }

    // Walks parent links from this folder up to root, collecting names root-first. Stops at
    // root, at a parent that is not a known folder (partial path from resolvable ancestors),
    // or on a cycle (corrupt tree). Mirrors the walk's "/"-join so strings match exactly.
    private static String buildPath(final int folderId, final int rootId,
                                    final Map<Integer, Integer> parentOf, final Map<Integer, String> nameOf) {
        final Deque<String> names = new ArrayDeque<>();
        final Set<Integer> seen = new HashSet<>();
        int current = folderId;
        while (current != rootId && parentOf.containsKey(current) && seen.add(current)) {
            names.push(nameOf.getOrDefault(current, UNREADABLE_NAME));
            current = parentOf.get(current);
        }
        if (names.isEmpty()) {
            return "/";
        }
        return "/" + String.join("/", names);
    }

    private static String displayName(final PSTFile pst, final int descriptorId) {
        try {
            final PSTObject object = PSTObject.detectAndLoadPSTObject(pst, (long) descriptorId);
            final String name = (object instanceof PSTFolder) ? ((PSTFolder) object).getDisplayName() : null;
            return (name == null || name.isEmpty()) ? UNREADABLE_NAME : name;
        } catch (final Exception | LinkageError e) {
            return UNREADABLE_NAME;
        }
    }
}
