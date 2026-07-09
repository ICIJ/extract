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

    /**
     * Write a minimal, contentless "stub" for a container ROOT whose full write never happened because
     * the parse was aborted (timeout/cancel/crash) after some of its embeds had already been streamed to
     * the index. Writing the stub keeps those embeds from being orphaned under a root that does not
     * exist; a later re-run of the stage replaces the stub with the fully-parsed root.
     *
     * <p>Default no-op: only index-backed spewers, where orphaning is observable, need to act. Called at
     * most once per aborted root, after the streaming spew worker has drained.
     *
     * @param root            the container root whose full write never happened
     * @param writtenChildren the number of embedded children actually written to the endpoint before
     *                        the abort, so the stub can record how much of the container was recovered
     */
    protected void writeRootStub(final TikaDocument root, final long writtenChildren) throws IOException {
        // no-op by default
    }

    /**
     * Finalize a container ROOT that was fully written on the normal streaming path, once the spew
     * worker has drained and the child count is final. Lets an index-backed spewer record how many
     * children were indexed under the root and mark it complete. Called at most once per root, only
     * when the root was written successfully and had at least one child written (so it is a container).
     *
     * <p>Default no-op. Unlike {@link #writeRootStub} (the aborted path), this runs on successful
     * completion: the root document already exists, so implementations update it rather than create it.
     *
     * @param root            the container root already written during the parse
     * @param writtenChildren the total number of embedded children written under this root
     */
    protected void finalizeRoot(final TikaDocument root, final long writtenChildren) throws IOException {
        // no-op by default
    }

    public void write(final TikaDocument document) throws IOException {
        try {
            writeDocument(document, null, null, 0);
            for (EmbeddedTikaDocument childDocument : document.getEmbeds()) {
                writeTree(childDocument, document, document, 1);
            }
        } finally {
            // Closing the root reader releases any embedded-text temp files spilled past the
            // in-memory budget (see ResourceClosingReader). Owning cleanup here means every
            // spewer benefits, not just one Extractor entry point.
            closeReaderQuietly(document);
        }
    }

    private void writeTree(final TikaDocument doc, final TikaDocument parent, TikaDocument root, final int level)
            throws IOException {
        try {
            writeDocument(doc, parent, root, level);

            for (EmbeddedTikaDocument child : doc.getEmbeds()) {
                writeTree(child, doc, root, level + 1);
            }
        } finally {
            // Release this embedded document's reader as soon as its subtree is written. When its
            // text spilled to disk the reader is an open file handle; without this a large container
            // (PST/mailbox with tens of thousands of items) holds them all open at once and exhausts
            // the process file-descriptor limit.
            closeReaderQuietly(doc);
        }
    }

    protected static void closeReaderQuietly(final TikaDocument document) {
        try {
            final Reader reader = document.getReader();
            if (null != reader) {
                reader.close();
            }
        } catch (final IOException e) {
            // Best-effort: the content has already been written, so a failed close must not
            // mask the write outcome.
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
            // Emit a JSON array of ISO instants only when every value parses as a date
            // (all-or-nothing); any non-date value keeps the field comma-joined so we never
            // send a partially-valid date array to Elasticsearch.
            MetadataTransformer.toIso8601Array(values)
                    .ifPresentOrElse(
                            iso -> map.put(name, iso),
                            () -> map.put(name, String.join(",", values)));
        }
    }

    @Override
    public void close() throws Exception {}
}
