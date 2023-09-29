package org.icij.spewer;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class MetadataBlacklist {
    static final String METADATA_BLACKLIST_FILE = "/metadata_blacklist";
    private final List<String> blacklist;

    public MetadataBlacklist() {
        blacklist = load(getClass().getResource(METADATA_BLACKLIST_FILE));
    }

    public Boolean ok(String value) {
        return blacklist.stream().allMatch(pattern -> {
            String globPattern = "glob:" + pattern;
            return ok(value, globPattern);
        });
    }

    public Boolean ok(String value, String globPattern) {
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(globPattern);
        Path valueAsPath = FileSystems.getDefault().getPath(value);
        // To be "ok", a value must NOT match with the given glob pattern
        return !matcher.matches(valueAsPath);
    }


    public static List<String> load(URL resourceUrl) {
        if (resourceUrl == null) {
            return new ArrayList<>();
        }
        return load(Paths.get(resourceUrl.getPath()));
    }

    public static List<String> load(Path path) {
        try {
            return Files.readAllLines(path).stream().map(String::trim).collect(Collectors.toList());
        } catch (IOException e) {
            return new ArrayList<>();
        }
    }
}
