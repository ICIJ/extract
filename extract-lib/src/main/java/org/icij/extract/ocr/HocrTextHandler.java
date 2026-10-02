package org.icij.extract.ocr;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class HocrTextHandler extends DefaultHandler {
    private static final Pattern WORD_CONFIDENCE = Pattern.compile("x_wconf (\\d+)");
    // Tesseract classes a text line by its layout role, not always as ocr_line.
    private static final Set<String> LINES = Set.of("ocr_line", "ocr_header", "ocr_caption", "ocr_textfloat");
    private static final String PARAGRAPH_BREAK = "\n\n";

    private final StringBuilder text = new StringBuilder();
    private final StringBuilder word = new StringBuilder();
    private String separator = "";
    private boolean isPageStart = false;
    private int depth = 0;
    private int wordDepth = -1;
    private long confidenceSum = 0;
    private int confidenceCount = 0;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        depth++;
        String hocrClass = attributes.getValue("class");
        if ("ocr_page".equals(hocrClass)) {
            isPageStart = true;
        } else if ("ocr_par".equals(hocrClass)) {
            // Tesseract's text output puts a single newline between pages, not a paragraph break.
            separator = isPageStart ? "\n" : PARAGRAPH_BREAK;
            isPageStart = false;
        } else if (hocrClass != null && LINES.contains(hocrClass) && !separator.equals(PARAGRAPH_BREAK)) {
            separator = "\n";
        } else if ("ocrx_word".equals(hocrClass)) {
            wordDepth = depth;
            addConfidence(attributes.getValue("title"));
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        if (depth == wordDepth) {
            appendWord();
            wordDepth = -1;
        }
        depth--;
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        if (wordDepth != -1) {
            word.append(ch, start, length);
        }
    }

    String text() {
        return text.toString();
    }

    double meanConfidence() {
        return confidenceCount == 0 ? 0 : (double) confidenceSum / confidenceCount;
    }

    private void addConfidence(String title) {
        Matcher matcher = WORD_CONFIDENCE.matcher(Objects.toString(title, ""));
        if (matcher.find()) {
            confidenceSum += Integer.parseInt(matcher.group(1));
            confidenceCount++;
        }
    }

    private void appendWord() {
        String value = word.toString().strip();
        word.setLength(0);
        if (value.isEmpty()) {
            return;
        }
        if (!text.isEmpty()) {
            text.append(separator);
        }
        text.append(value);
        separator = " ";
    }
}
