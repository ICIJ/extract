package org.icij.extract.extractor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class TikaVersionTestHelper {
    private static final String POM_PROPERTIES_PATH = "META-INF/maven/org.apache.tika/tika-core/pom.properties";
    private static String originalVersion;

    private static String getCurrentVersion() throws IOException {
        Properties properties = new Properties();
        try (InputStream is = loadPOMProperties().openStream()) {
            properties.load(is);
        }
        return properties.getProperty("version");
    }

    public static void setVersion(String version) throws IOException {
        if (originalVersion == null) {
            originalVersion = getCurrentVersion();
        }
        writeVersion(version);
    }

    public static void restoreVersion() throws IOException {
        if (originalVersion != null) {
            writeVersion(originalVersion);
            originalVersion = null;
        }
    }

    private static void writeVersion(String version) throws IOException {
        Path pomPropertiesPath = Paths.get(loadPOMProperties().getPath());

        String content = String.format("groupId=org.apache.tika%nartifactId=tika-core%nversion=%s%n", version);
        Files.write(pomPropertiesPath, content.getBytes(StandardCharsets.UTF_8));
    }

    private static URL loadPOMProperties() throws IOException {
        URL pomProperties = TikaVersionTestHelper.class.getClassLoader().getResource(POM_PROPERTIES_PATH);
        if (pomProperties == null) {
            throw new IOException("Cannot find " + POM_PROPERTIES_PATH);
        }
        return pomProperties;
    }
}
