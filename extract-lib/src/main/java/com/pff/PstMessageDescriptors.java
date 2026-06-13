package com.pff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Helper placed in package {@code com.pff} so it can read java-libpst's
 * package-private descriptor tree. Returns the descriptor identifiers of every
 * normal mail message in the PST (both folder-reachable and orphaned/deleted),
 * filtered cheaply by node type so message bodies are never loaded here.
 */
public final class PstMessageDescriptors {

    private PstMessageDescriptors() {
    }

    public static List<Integer> normalMessageDescriptorIds(final PSTFile pst) throws IOException, PSTException {
        final List<Integer> result = new ArrayList<>();
        for (LinkedList<DescriptorIndexNode> nodes : pst.getChildDescriptorTree().values()) {
            for (DescriptorIndexNode descriptor : nodes) {
                if (PSTObject.getNodeType(descriptor.descriptorIdentifier) == PSTObject.NID_TYPE_NORMAL_MESSAGE) {
                    result.add(descriptor.descriptorIdentifier);
                }
            }
        }
        return result;
    }
}
