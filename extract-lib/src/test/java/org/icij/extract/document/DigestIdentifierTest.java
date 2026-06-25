package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class DigestIdentifierTest {

    private static final String ALGO = "SHA-256";

    // A root document whose content hash is fixed, so embed ids (which mix in the parent id) are
    // reproducible across separately-built parents.
    private TikaDocument parentWithHash() {
        DigestIdentifier identifier = new DigestIdentifier(ALGO, StandardCharsets.UTF_8);
        TikaDocument parent = new DocumentFactory().withIdentifier(identifier).create(Paths.get("parent.pst"));
        parent.getMetadata().set(Identifier.getKey(ALGO), "PARENTHASH");
        return parent;
    }

    private EmbeddedTikaDocument embed(TikaDocument parent, String name, String relationshipId, String contentHash) {
        Metadata m = new Metadata();
        if (name != null) m.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        if (relationshipId != null) m.set(TikaCoreProperties.EMBEDDED_RELATIONSHIP_ID, relationshipId);
        if (contentHash != null) m.set(Identifier.getKey(ALGO), contentHash);
        return parent.addEmbed(m);
    }

    // A content-less embed (e.g. an unreadable PST/OST by-value attachment, or a recovery stub) has
    // no file digest. generateForEmbed must not throw; it derives the id from parent + relId + name.
    @Test
    public void embed_without_content_hash_gets_an_id_instead_of_throwing() {
        String id = embed(parentWithHash(), "1.jpg", "123-0", null).getId();
        assertThat(id).isNotEmpty();
    }

    @Test
    public void content_less_embed_id_is_deterministic_across_parses() {
        String first = embed(parentWithHash(), "1.jpg", "123-0", null).getId();
        String second = embed(parentWithHash(), "1.jpg", "123-0", null).getId();
        assertThat(first).isEqualTo(second);
    }

    @Test
    public void content_less_embeds_with_distinct_relationship_ids_get_distinct_ids() {
        TikaDocument parent = parentWithHash();
        String a = embed(parent, "1.jpg", "123-0", null).getId();
        String b = embed(parent, "1.jpg", "123-1", null).getId();
        assertThat(a).isNotEqualTo(b);
    }

    // Regression: when a content hash is present the id is unchanged from the historical behaviour
    // (hash + parent + relId + name), so the fix only affects the previously-throwing null-hash case.
    @Test
    public void embed_with_content_hash_still_produces_an_id() {
        String id = embed(parentWithHash(), "doc.pdf", "9-0", "CONTENTHASH").getId();
        assertThat(id).isNotEmpty();
    }
}
