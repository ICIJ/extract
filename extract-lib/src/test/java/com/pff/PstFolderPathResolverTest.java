package com.pff;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PST;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.parser.ResilientOutlookPSTParser;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

public class PstFolderPathResolverTest {

    private Path testPst() throws Exception {
        return Paths.get(getClass().getResource("/documents/pst/testPST.pst").toURI());
    }

    // Captures the folder path stamped on every emitted pst-mail-item by the parser's walk.
    private static final class FolderPathCapturingExtractor implements EmbeddedDocumentExtractor {
        final List<String> folderPaths = new ArrayList<>();

        public boolean shouldParseEmbedded(Metadata metadata) {
            return true;
        }

        public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) {
            String type = metadata.get(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE);
            if (type != null && type.contains("pst-mail-item")) {
                folderPaths.add(metadata.get(PST.PST_FOLDER_PATH));
            }
        }
    }

    @Test
    public void resolverComputesSameFolderPathsAsTheWalk() throws Exception {
        // Ground truth: the paths the parser's folder walk actually emits.
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        FolderPathCapturingExtractor capture = new FolderPathCapturingExtractor();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, capture);
        Metadata metadata = new Metadata();
        try (InputStream is = TikaInputStream.get(testPst(), metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }

        // Resolver-computed path for each visible message: folderPaths[message.parent].
        PSTFile pst = new PSTFile(testPst().toString());
        try {
            Map<Integer, String> folderPaths = PstFolderPathResolver.folderPaths(pst);
            List<String> resolved = new ArrayList<>();
            for (LinkedList<DescriptorIndexNode> nodes : pst.getChildDescriptorTree().values()) {
                for (DescriptorIndexNode node : nodes) {
                    if (PSTObject.getNodeType(node.descriptorIdentifier) == PSTObject.NID_TYPE_NORMAL_MESSAGE) {
                        String path = folderPaths.get(node.parentDescriptorIndexIdentifier);
                        if (path != null) {
                            resolved.add(path);
                        }
                    }
                }
            }

            List<String> expected = new ArrayList<>(capture.folderPaths);
            Collections.sort(expected);
            Collections.sort(resolved);
            assertThat(resolved).isEqualTo(expected);
            assertThat(resolved).hasSize(7);
        } finally {
            pst.getFileHandle().close();
        }
    }
}
