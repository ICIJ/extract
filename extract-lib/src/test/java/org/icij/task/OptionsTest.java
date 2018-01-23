package org.icij.task;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class OptionsTest {
    @Test
    public void test_from_map() {
        Map<String, String> properties = new HashMap<String, String>() {{
            put("key1", "value1");
            put("key2", "value2");
        }};

        Options<String> options = Options.from(properties);
        assertEquals("value1", options.get("key1").value().get());
        assertEquals("value2", options.get("key2").value().get());
    }

    @Test
    public void test_from_properties() {
        Properties properties = new Properties();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        Options<String> options = Options.from(properties);
        assertEquals("value1", options.get("key1").value().get());
        assertEquals("value2", options.get("key2").value().get());
    }

    @Test
    public void test_option_equals() throws Exception {
        Option<String> opt1 = new Option<>("key", StringOptionParser::new).update("value");
        Option<String> opt2 = new Option<>("key", StringOptionParser::new).update("value");
        assertEquals(opt1, opt2);
    }

    @Test
    public void test_empty_options_toString() {
        assertEquals("{}", new Options().toString());
    }

    @Test
    public void test_toString() {
        Options<String> options = Options.from(new HashMap<String, String>() {{
            put("key1", "value1");
            put("key2", "value2");
        }});
        assertEquals("{key1=value1,key2=value2}", options.toString());
    }

    @Test
    public void test_options_equals() {
        HashMap<String, String> stringProperties = new HashMap<String, String>() {{
            put("key1", "value1");
            put("key2", "value2");
        }};
        assertEquals(Options.from(stringProperties), Options.from(stringProperties));
    }

    @Test
    public void test_createFrom() throws Exception {
        Options<String> options1 = Options.from(new HashMap<String, String>() {{
            put("key1", "value1");
            put("key2", "value2");
        }});
        Options<String> options2 = Options.from(new HashMap<String, String>() {{
            put("key1", "new value1");
        }});

        assertEquals(Options.from(new HashMap<String, String>() {{
                    put("key1", "new value1");
                    put("key2", "value2");
                }}), options1.createFrom(options2));
    }
}