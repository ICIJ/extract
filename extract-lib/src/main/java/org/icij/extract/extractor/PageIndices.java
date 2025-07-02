package org.icij.extract.extractor;

import java.util.List;

public record PageIndices (String extractor, List<Pair<Long, Long>> pages) { }
