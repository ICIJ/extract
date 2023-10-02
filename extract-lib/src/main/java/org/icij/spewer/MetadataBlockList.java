package org.icij.spewer;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class MetadataBlockList {
    static final Charset METADATA_BLOCK_LIST_CHARSET = StandardCharsets.UTF_8;
    static final String METADATA_BLOCK_LIST_FILE = "metadata_block_list.txt";
    private final List<String> list;

    public MetadataBlockList() {
        list = load(METADATA_BLOCK_LIST_FILE, METADATA_BLOCK_LIST_CHARSET);
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

    public static List<String> load(String resourceName, Charset charset) {
        URL resource = ClassLoader.getSystemClassLoader().getResource(resourceName);
        if (resource == null) {
            return new ArrayList<>();
        }
        return load(resource, charset);
    }

    public static List<String> load(URL resource, Charset charset) {
        try (InputStream is = resource.openStream())  {
            return IOUtils
                    .readLines(is, charset)
                    .stream()
                    .map(String::trim)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            return new ArrayList<>();
        }
    }
}
