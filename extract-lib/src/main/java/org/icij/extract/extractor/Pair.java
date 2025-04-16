package org.icij.extract.extractor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;

@JsonSerialize(using = Pair.PairSerializer.class)
public class Pair<L, R> extends org.apache.commons.lang3.tuple.ImmutablePair<L, R> {
    public Pair(L left, R right) {
        super(left, right);
    }

    static final class PairSerializer<L, R> extends JsonSerializer<Pair<L, R>> {
        @Override
        public void serialize(Pair<L, R> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartArray();
            gen.writeObject(value.getLeft());
            gen.writeObject(value.getRight());
            gen.writeEndArray();
        }
    }
}
