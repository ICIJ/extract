package org.icij.extract.extractor;

import org.apache.commons.codec.binary.Hex;
import org.apache.tika.parser.digest.CompositeDigester;

public class UpdatableDigester extends CompositeDigester {
    public UpdatableDigester(final String modifier, final String algoName) {
        super(new UpdatableInputStreamDigester(20 * 1024 * 1024,
                algoName, algoName.replace("-", ""), Hex::encodeHexString) {
            @Override
            protected String getDigestUpdateModifier() {
                return modifier;
            }
        });
    }
}
