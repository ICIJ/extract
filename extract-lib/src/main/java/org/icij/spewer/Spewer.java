package org.icij.spewer;

import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.parser.ParsingReader;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for {@linkplain Spewer} superclasses that write text output from a {@link ParsingReader} to specific
 * endpoints.
 *
 * @since 1.0.0-beta
 */
@Option(name = "outputMetadata", description = "Output metadata along with extracted text. For the " +
        "\"file\" output type, a corresponding JSON file is created for every input file. With indexes, metadata " +
        "fields are set using an optional prefix. On by default.")
@Option(name = "tag", description = "Set the given field to a corresponding value on each document output.",
        parameter = "name-value-pair")
@Option(name = "charset", description = "Set the output encoding for text and document attributes. Defaults to UTF-8.",
		parameter = "name")
public abstract class Spewer implements AutoCloseable, Serializable {
    private static final long serialVersionUID = 5169670165236652447L;

    protected boolean outputMetadata = true;
    private Charset outputEncoding = StandardCharsets.UTF_8;
    protected final Map<String, String> tags = new HashMap<>();
    protected final FieldNames fields;

    public Spewer() { this(new FieldNames());}
    public Spewer(final FieldNames fields) {
        this.fields = fields;
    }

    public Spewer configure(final Options<String> options) {
        options.get("outputMetadata", "false").parse().asBoolean().ifPresent(this::outputMetadata);
        options.get("charset", StandardCharsets.UTF_8.toString()).value(Charset::forName).ifPresent(this::setOutputEncoding);
        if (null != options.get("tag")) {
            options.get("tag", ":").values().forEach(this::setTag);
        }
        return this;
    }

    protected abstract void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException;

    public void write(final TikaDocument document) throws IOException {
        writeDocument(document, null, null, 0);
        for (EmbeddedTikaDocument childDocument : document.getEmbeds()) {
            writeTree(childDocument, document, document, 1);
        }
    }

    private void writeTree(final TikaDocument doc, final TikaDocument parent, TikaDocument root, final int level)
            throws IOException {
        writeDocument(doc, parent, root, level);

        for (EmbeddedTikaDocument child : doc.getEmbeds()) {
            writeTree(child, doc, root, level + 1);
        }
    }

    public TikaDocument[] write(final Path path) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    public FieldNames getFields() {
        return fields;
    }

    public void setOutputEncoding(final Charset outputEncoding) {
        this.outputEncoding = outputEncoding;
    }

    public Charset getOutputEncoding() {
        return outputEncoding;
    }

    public void outputMetadata(final boolean outputMetadata) {
        this.outputMetadata = outputMetadata;
    }

    public boolean outputMetadata() {
        return outputMetadata;
    }

    public void setTags(final Map<String, String> tags) {
        tags.forEach(this::setTag);
    }

    public Map<String, String> getTags() {
        return tags;
    }

    private void setTag(final String name, final String value) {
        tags.put(name, value);
    }

    private void setTag(final String tag) {
        final String[] pair = tag.split(":", 2);

        if (2 == pair.length) {
            setTag(pair[0], pair[1]);
        } else {
            throw new IllegalArgumentException(String.format("Invalid tag pair: \"%s\".", tag));
        }
    }

    protected void copy(final Reader input, final OutputStream output) throws IOException {
        copy(input, new OutputStreamWriter(output, outputEncoding));
    }

    public static void copy(final Reader input, final Writer output) throws IOException {
        final char[] buffer = new char[1024];
        int n;

        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }

        output.flush();
    }

    public static String toString(final Reader reader) throws IOException {
        final StringWriter writer = new StringWriter(4096);

        copy(reader, writer);
        return writer.toString();
    }

    protected Map<String, Object> getMetadata(TikaDocument document) throws IOException {
        Map<String, Object> metadata = new HashMap<>();
        new MetadataTransformer(document.getMetadata(), fields).transform(
                new MapValueConsumer(metadata), new MapValuesConsumer(metadata));
        return metadata;
    }

    static protected class MapValueConsumer implements MetadataTransformer.ValueConsumer {

        private final Map<String, Object> map;

        MapValueConsumer(final Map<String, Object> map) { this.map = map;}

        @Override
        public void accept(String name, String value) throws IOException {
            map.put(name, value);
        }
    }

    static protected class MapValuesConsumer implements MetadataTransformer.ValueArrayConsumer {

        private final Map<String, Object> map;

        MapValuesConsumer(Map<String, Object> jsonDocument) { map = jsonDocument;}

        @Override
        public void accept(String name, String[] values) throws IOException {
            map.put(name, String.join(",", values));
        }
    }

    @Override
    public void close() throws Exception {}
}
