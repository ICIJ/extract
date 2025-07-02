package org.icij.extract.extractor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

import java.io.IOException;

import static org.apache.poi.hslf.record.RecordTypes.List;

@JsonSerialize(using = Pair.PairSerializer.class)
@JsonDeserialize(using = Pair.PairDeserializer.class)
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

    static final class PairDeserializer<L, R> extends JsonDeserializer<Pair<L, R>> implements ContextualDeserializer {
        private final JavaType leftType;
        private final JavaType rightType;

        public PairDeserializer() {
            this(null, null);
        }

        PairDeserializer(JavaType leftType, JavaType rightType) {
            this.leftType = leftType;
            this.rightType = rightType;
        }

        @Override
        public Pair<L, R> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
            if (jsonParser.currentToken() != JsonToken.START_ARRAY) {
                throw new IllegalArgumentException("Expected START_ARRAY but got " + jsonParser.getCurrentToken());
            }
            jsonParser.nextToken();
            L left = mapper.readValue(jsonParser, leftType);
            R right = mapper.readValue(jsonParser, rightType);
            jsonParser.nextToken();
            return new Pair<>(left, right);
        }

        @Override
        public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
            JavaType contextualType = property != null ? property.getType() : ctxt.getContextualType();

            if (contextualType.containedTypeCount() != 2 &&
                    (List.getClass().isAssignableFrom(contextualType.getRawClass()) && contextualType.containedType(0).containedTypeCount() != 2)) {
                throw new JsonMappingException(ctxt.getParser(), "Pair must have two generic types");
            }
            JavaType left;
            JavaType right;
            if (contextualType.containedTypeCount() == 2) {
                left = contextualType.containedType(0);
                right = contextualType.containedType(1);
            } else {
                // collection (with one container type)
                left = contextualType.containedType(0).containedType(0);
                right = contextualType.containedType(0).containedType(1);
            }

            return new PairDeserializer<>(left, right);
        }
    }
}
