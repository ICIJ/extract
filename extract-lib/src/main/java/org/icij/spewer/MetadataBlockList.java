package org.icij.spewer;

import java.io.IOException;
import java.net.URL;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class MetadataBlockList {
    static final String METADATA_BLOCK_LIST_FILE = "/metadata_block_list";
    private final List<String> list;

    public MetadataBlockList() {
        list = load(getClass().getResource(METADATA_BLOCK_LIST_FILE));
    }

    public Boolean ok(String value) {
        return list.stream().allMatch(pattern -> {
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
