package ldbc.snb.datagen.serializer.yarspg.property;

import ldbc.snb.datagen.serializer.yarspg.Wrapper;

import java.util.Collection;

public class Property {
    private final String name;
    private PrimitiveProperty primitive;
    private ComplexProperty complex;

    Property(String name) {
        this.name = name;
    }

    public PrimitiveProperty getPrimitive() {
        return primitive;
    }

    public ComplexProperty getComplex() {
        return complex;
    }

    public String getName() {
        return name;
    }

    public void withComplex(ComplexProperty complex) {
        this.complex = complex;
    }

    public void generateComplex(ComplexType complexType, PrimitiveType primitiveType, String value) {
        primitive = new PrimitiveProperty(primitiveType, value);
        complex = new ComplexProperty(complexType, primitive);
    }

    public void generateComplex(ComplexType complexType, PrimitiveType primitiveType, Collection<String> values) {
        primitive = new PrimitiveProperty(primitiveType, values);
        complex = new ComplexProperty(complexType, primitive);
    }

    public void withPrimitive(PrimitiveProperty primitive) {
        this.primitive = primitive;
    }

    // TODO: docs
    public PrimitiveProperty generatePrimitive(PrimitiveType primitiveType, String value) {
        primitive = new PrimitiveProperty(primitiveType, value);

        return primitive;
    }

    // TODO: docs
    public PrimitiveProperty generatePrimitive(PrimitiveType primitiveType, Collection<String> values) {
        return primitive = new PrimitiveProperty(primitiveType, values);
    }

    @Override
    public String toString() {
        if (complex == null) {
            return Wrapper.wrapProperty(name, primitive.toString());
        }

        return Wrapper.wrapProperty(name, complex.toString());
    }
}



