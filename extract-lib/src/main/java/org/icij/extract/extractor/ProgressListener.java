package org.icij.extract.extractor;

import java.util.Collection;

@FunctionalInterface
public interface ProgressListener {
    void onHeartbeat(Collection<ExtractionProgress> inFlight);
}
