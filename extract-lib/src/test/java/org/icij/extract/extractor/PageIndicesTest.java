package org.icij.extract.extractor;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public class PageIndicesTest {
    @Test
    public void test_deserialize() throws IOException {
        PageIndices pageIndices = new ObjectMapper().readValue("""
                {
                   "extractor": "Tika-3.0.1",
                   "pages": [
                      [0, 123],
                      [124, 432],
                      [433, 654]
                   ]
                }
                """, PageIndices.class);
        assertThat(pageIndices.extractor()).isEqualTo("Tika-3.0.1");
        assertThat(pageIndices.pages()).hasSize(3);
    }
}