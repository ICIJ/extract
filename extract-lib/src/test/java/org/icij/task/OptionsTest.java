package org.icij.task;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class OptionsTest {
    @Test
    public void testFromProperties() {
        Properties properties = new Properties();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        Options<String> options = Options.from(properties);
        assertEquals("value1", options.get("key1").value().get());
        assertEquals("value2", options.get("key2").value().get());
    }
}