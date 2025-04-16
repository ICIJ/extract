package org.icij.extract.extractor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class PairTest {
    @Test
    public void test_serialize() throws JsonProcessingException {
        assertThat(new ObjectMapper().writeValueAsString(new Pair<>("foo", "bar"))).isEqualTo("[\"foo\",\"bar\"]");
        assertThat(new ObjectMapper().writeValueAsString(new Pair<>(12, 34))).isEqualTo("[12,34]");
    }
}
