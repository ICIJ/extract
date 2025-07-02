package org.icij.extract.extractor;

import java.nio.file.Path;

public class ArtifactUtils {
    static Path getEmbeddedPath(Path artifactPath, String digest) {
        return artifactPath.resolve(digest.substring(0, 2)).resolve(digest.substring(2, 4)).resolve(digest);
    }
}
