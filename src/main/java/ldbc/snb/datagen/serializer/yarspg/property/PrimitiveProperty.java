package ldbc.snb.datagen.serializer.yarspg.property;

import ldbc.snb.datagen.serializer.yarspg.Wrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class PrimitiveProperty {
    private MetaProperty metaProperty;
    private PrimitiveType primitiveType;
    private List<String> values;

    public PrimitiveProperty(PrimitiveType primitiveType, Collection<String> values) {
        this.primitiveType = primitiveType;
        this.values = new ArrayList<>(values);
    }

    public PrimitiveProperty(PrimitiveType primitiveType, String values) {
        this.primitiveType = primitiveType;
        this.values = Collections.singletonList(values);
    }

    public PrimitiveType getPrimitiveType() {
        return primitiveType;
    }

    public List<String> getValues() {
        return values;
    }

    public MetaProperty getMetaProperty() {
        return metaProperty;
    }

    public PrimitiveProperty withMeta(String metaValue, Consumer<MetaProperty> consumer) {
        metaProperty = new MetaProperty(metaValue);
        consumer.accept(metaProperty);
        return this;
    }

    @Override
    public String toString() {
        return Wrapper.wrapProperties(values, primitiveType);
    }
}