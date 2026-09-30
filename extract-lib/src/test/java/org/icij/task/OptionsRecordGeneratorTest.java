package org.icij.task;

import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.tools.ToolProvider;
import java.lang.reflect.RecordComponent;
import java.net.URLClassLoader;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class OptionsRecordGeneratorTest {
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Option(name = "timeout", description = "a timeout", parameter = "duration")
    @Option(name = "label", description = "a label", parameter = "name")
    @OptionsClass(Nested.class)
    static class Annotated {}

    @Option(name = "dir", description = "a dir", parameter = "path")
    @Option(name = "flag", description = "a flag")
    static class Nested {}

    @Test
    public void test_generates_record_with_types_from_parameter() throws Exception {
        Class<?> record = compile(OptionsRecordGenerator.generate(Annotated.class, "AnnotatedOptions"), "AnnotatedOptions");

        Map<String, Class<?>> types = Arrays.stream(record.getRecordComponents())
                .collect(toMap(RecordComponent::getName, RecordComponent::getType));
        assertEquals(Map.of("timeout", Duration.class, "label", String.class, "dir", Path.class, "flag", String.class), types);
    }

    @Test
    public void test_from_and_to_options_round_trip() throws Exception {
        Class<?> record = compile(OptionsRecordGenerator.generate(Annotated.class, "AnnotatedOptions"), "AnnotatedOptions");
        Map<String, Object> values = Map.of("timeout", "5m", "label", "foo", "dir", "/tmp/bar", "unknown", "dropped");

        Object options = record.getMethod("from", Options.class).invoke(null, Options.from(values));
        Options<String> back = (Options<String>) record.getMethod("toOptions").invoke(options);

        Map<String, String> backValues = new HashMap<>();
        back.forEach(o -> backValues.put(o.name(), o.value().get()));
        // durations are written in ms: HumanDuration.format() truncates (90s -> "1m")
        assertEquals(Map.of("timeout", "300000ms", "label", "foo", "dir", "/tmp/bar"), backValues);
    }

    @Test
    public void test_write_creates_record_file_in_package_dir() throws Exception {
        Path srcDir = tmp.newFolder("java").toPath();

        Path written = OptionsRecordGenerator.write(Annotated.class, srcDir);

        assertEquals(srcDir.resolve("org/icij/task/AnnotatedOptions.java"), written);
        assertEquals(OptionsRecordGenerator.generate(Annotated.class, "AnnotatedOptions"), Files.readString(written));
    }

    @Test
    public void test_write_refuses_to_overwrite_existing_file() throws Exception {
        Path srcDir = tmp.newFolder("java").toPath();
        Path existing = Files.createDirectories(srcDir.resolve("org/icij/task")).resolve("AnnotatedOptions.java");
        Files.writeString(existing, "hand edited");

        assertThrows(FileAlreadyExistsException.class, () -> OptionsRecordGenerator.write(Annotated.class, srcDir));
        assertEquals("hand edited", Files.readString(existing));
    }

    private Class<?> compile(String source, String recordName) throws Exception {
        Path src = tmp.newFolder("src").toPath().resolve(recordName + ".java");
        Files.writeString(src, source);
        Path out = tmp.newFolder("out").toPath();
        int status = ToolProvider.getSystemJavaCompiler().run(null, null, null,
                "-d", out.toString(), "-cp", System.getProperty("java.class.path"), src.toString());
        assertEquals("generated source must compile:\n" + source, 0, status);
        return new URLClassLoader(new java.net.URL[]{out.toUri().toURL()}, getClass().getClassLoader())
                .loadClass("org.icij.task." + recordName);
    }
}
