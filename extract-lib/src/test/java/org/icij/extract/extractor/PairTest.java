package org.icij.extract.extractor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class PairTest {
    @Test
    public void test_serialize() throws JsonProcessingException {
        assertThat(new ObjectMapper().writeValueAsString(new Pair<>("foo", "bar"))).isEqualTo("[\"foo\",\"bar\"]");
        assertThat(new ObjectMapper().writeValueAsString(new Pair<>(12, 34))).isEqualTo("[12,34]");
    }
    @Test
    public void test_deserialize() throws JsonProcessingException {
        assertThat(new ObjectMapper().<Pair<Integer, Integer>>readValue("[12,34]", new TypeReference<>() {})).isEqualTo(new Pair<>(12, 34));
        assertThat(new ObjectMapper().<Pair<Long, Long>>readValue("[12,34]", new TypeReference<>() {})).isEqualTo(new Pair<>(12L, 34L));
        assertThat(new ObjectMapper().<Pair<String, String>>readValue("[\"foo\",\"bar\"]", new TypeReference<>() {})).isEqualTo(new Pair<>("foo", "bar"));
    }
}