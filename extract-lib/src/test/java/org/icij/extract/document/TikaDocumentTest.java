package org.icij.extract.document;


import org.apache.tika.Tika;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static org.fest.assertions.Assertions.assertThat;

public class TikaDocumentTest {
    @Test
    public void test_get_tika_version() {
        assertThat(TikaDocument.getTikaVersion(new Date(0))).isEqualTo("Apache Tika 1.8.0");
        assertThat(TikaDocument.getTikaVersion(Date.from(Instant.parse("2016-05-24T15:29:30Z")))).isEqualTo("Apache Tika 1.12.0");
        assertThat(TikaDocument.getTikaVersion(Date.from(Instant.parse("2016-05-24T15:29:32Z")))).isEqualTo("Apache Tika 1.13.0");
        assertThat(TikaDocument.getTikaVersion(new Date())).isEqualTo(Tika.getString());
    }
}