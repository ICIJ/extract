package org.icij.extract.extractor;

import org.apache.commons.codec.binary.Hex;
import org.apache.tika.parser.digest.CompositeDigester;

public class UpdatableDigester extends CompositeDigester {
    final String algorithm;

    public UpdatableDigester(final String modifier, final String algorithm) {
        super(new UpdatableInputStreamDigester(20 * 1024 * 1024,
                algorithm, algorithm.replace("-", ""), Hex::encodeHexString) {
            @Override
            protected String getDigestUpdateModifier() {
                return modifier;
            }
        });
        this.algorithm = algorithm;
    }
}
