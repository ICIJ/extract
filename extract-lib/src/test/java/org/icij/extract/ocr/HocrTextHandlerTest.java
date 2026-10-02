package org.icij.extract.ocr;

import org.junit.Test;

import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fest.assertions.Assertions.assertThat;

public class HocrTextHandlerTest {
    @Test
    public void test_rebuilds_lines_and_paragraphs_and_averages_word_confidence() throws Exception {
        // Given
        String hocr = """
            <html xmlns="http://www.w3.org/1999/xhtml"><body>
             <div class='ocr_page' title='bbox 0 0 100 100'>
              <p class='ocr_par'>
               <span class='ocr_line'><span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 90'>Hello</span> <span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 80'>world</span></span>
               <span class='ocr_line'><span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 70'>again</span></span>
              </p>
              <p class='ocr_par'>
               <span class='ocr_header'><span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 60'><strong>Title</strong></span></span>
              </p>
             </div>
            </body></html>
            """;
        HocrTextHandler handler = new HocrTextHandler();
        // When
        parse(hocr, handler);
        // Then
        assertThat(handler.text()).isEqualTo("Hello world\nagain\n\nTitle");
        assertThat(handler.meanConfidence()).isEqualTo(75.0);
    }

    @Test
    public void test_a_page_without_words_has_no_text_and_zero_confidence() throws Exception {
        // Given
        String hocr = "<html xmlns=\"http://www.w3.org/1999/xhtml\"><body>" +
                "<div class='ocr_page' title='bbox 0 0 100 100'></div></body></html>";
        HocrTextHandler handler = new HocrTextHandler();
        // When
        parse(hocr, handler);
        // Then
        assertThat(handler.text()).isEmpty();
        assertThat(handler.meanConfidence()).isEqualTo(0.0);
    }

    @Test
    public void test_pages_are_separated_by_a_single_newline() throws Exception {
        // Given
        String hocr = """
            <html xmlns="http://www.w3.org/1999/xhtml"><body>
             <div class='ocr_page' title='bbox 0 0 100 100'>
              <p class='ocr_par'><span class='ocr_line'><span class='ocrx_word' title='x_wconf 90'>Page</span> <span class='ocrx_word' title='x_wconf 90'>1</span></span></p>
             </div>
             <div class='ocr_page' title='bbox 0 0 100 100'>
              <p class='ocr_par'><span class='ocr_line'><span class='ocrx_word' title='x_wconf 90'>Multipage</span></span></p>
             </div>
            </body></html>
            """;
        HocrTextHandler handler = new HocrTextHandler();
        // When
        parse(hocr, handler);
        // Then
        assertThat(handler.text()).isEqualTo("Page 1\nMultipage");
    }

    private static void parse(String hocr, HocrTextHandler handler) throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.newSAXParser().parse(new ByteArrayInputStream(hocr.getBytes(UTF_8)), handler);
    }
}
