package org.icij.extract.ocr;

import java.util.List;

/** Parses the ocrPageSegMode option into the tesseract page segmentation modes {@link PageSegModeOCRParser} tries. */
public class PageSegModes {
    /** Modes AUTO tries in order: page layout, then a single text block, then sparse text. */
    public static final List<String> AUTO = List.of("1", "6", "11");

    private PageSegModes() {}

    public static List<String> parse(String value) {
        if (value.strip().equalsIgnoreCase("AUTO")) {
            return AUTO;
        }
        return List.of(requireTextReadingMode(value));
    }

    // 0 only detects orientation and script, 2 is not implemented in tesseract.
    private static String requireTextReadingMode(String value) {
        String mode = value.strip();
        if (!mode.matches("1|[3-9]|1[0-3]")) {
            throw new InvalidPageSegMode(value);
        }
        return mode;
    }
}
