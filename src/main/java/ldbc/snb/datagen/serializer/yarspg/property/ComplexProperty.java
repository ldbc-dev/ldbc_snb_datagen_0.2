package ldbc.snb.datagen.serializer.yarspg.property;

import ldbc.snb.datagen.serializer.yarspg.Wrapper;

import java.util.Arrays;
import java.util.List;
import java.util.MissingFormatArgumentException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ComplexProperty {
    private MetaProperty metaProperty;
    private ComplexType complexType;
    private PrimitiveProperty primitive;
    private List<ComplexProperty> complexList;
    private String key;

    public ComplexProperty(ComplexType complexType, PrimitiveProperty primitive) {
        this.complexType = complexType;
        this.primitive = primitive;
    }

    public ComplexProperty(ComplexProperty... complexes) {
        // Previous value from recursion - easiest way
        complexType = complexes[0].getComplexType();
        complexList = Arrays.asList(complexes);
    }

    // Struct case
    public ComplexProperty(String key, ComplexType complexType, PrimitiveProperty primitive) {
        this.complexType = complexType;
        this.primitive = primitive;
        this.key = key;
    }

    public ComplexProperty(String key, ComplexProperty... complexes) {
        // Previous value from recursion - easiest way
        complexType = complexes[0].getComplexType();
        complexList = Arrays.asList(complexes);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public MetaProperty getMetaProperty() {
        return metaProperty;
    }

    public PrimitiveProperty getPrimitive() {
        return primitive;
    }

    public ComplexType getComplexType() {
        return complexType;
    }

    public List<ComplexProperty> getComplexList() {
        return complexList;
    }

    public ComplexProperty withKey(String key) {
        this.key = key;
        return this;
    }

    public ComplexProperty withMeta(String metaValue, Consumer<MetaProperty> consumer) {
        metaProperty = new MetaProperty(metaValue);
        consumer.accept(metaProperty);
        return this;
    }

    @Override
    public String toString() {
        String property = (primitive != null)
                ? primitive.toString()
                : complexList.stream()
                             .map(ComplexProperty::toString)
                             .collect(Collectors.joining(","));

        switch (complexType) {
            case SET:
                return Wrapper.wrapSet(property);
            case STRUCT:
                return Wrapper.wrapStruct(key, property);
            case LIST:
                return Wrapper.wrapList(property);
            default:
                throw new MissingFormatArgumentException("Complex type not provided");
        }
    }
}