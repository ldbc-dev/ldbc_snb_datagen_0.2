package ldbc.snb.datagen.serializer.yarspg.property.schema;

import ldbc.snb.datagen.serializer.yarspg.Wrapper;
import ldbc.snb.datagen.serializer.yarspg.property.ComplexType;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PropertySchema {
    private final String name;
    private PrimitiveSchema primitiveSchema;
    private List<ComplexSchema> complexSchemas;

    public PropertySchema(String name) {
        this.name = name;
    }

    // TODO: Docs
    public void generateSchema(Integer depth, ComplexType complexType, PrimitiveType primitiveType) {
        complexSchemas = new ArrayList<>();
        for (int i = 0; i < depth; i++) {
            complexSchemas.add(new ComplexSchema(complexType));
        }

        primitiveSchema = new PrimitiveSchema(primitiveType);
    }

    // TODO: Docs
    // Minimal amount of arguments validation
    public PrimitiveSchema generateSchema(PrimitiveType primitiveType) {
        return primitiveSchema = new PrimitiveSchema(primitiveType);
    }

    // TODO: Docs
    public PropertySchema withComplex(ComplexSchema... complexSchemas) {
        this.complexSchemas = Arrays.asList(complexSchemas);
        return this;
    }

    public void withPrimitive(PrimitiveSchema primitiveSchema) {
        this.primitiveSchema = primitiveSchema;
    }

    public String getName() {
        return name;
    }

    public PrimitiveSchema getSchemaPrimitive() {
        return primitiveSchema;
    }

    public List<ComplexSchema> getSchemaComplexes() {
        return complexSchemas;
    }

    // TODO: Docs
    // Not iterator (class) due to side effect of hasNext with recursion
    private String buildSchema(Integer deep) {
        if (deep < complexSchemas.size()) {
            ComplexSchema complexSchema = complexSchemas.get(deep);
            String metaSchemaString = (complexSchema.getMetaSchema() == null)
                    ? ""
                    : complexSchema.getMetaSchema()
                                   .toString();

            return complexSchema.getComplexType() + "(" + buildSchema(
                    deep + 1) + complexSchema + ")" + metaSchemaString;
        }

        return primitiveSchema.toString();
    }

    @Override
    public String toString() {
        if (complexSchemas == null) {
            return Wrapper.wrapProperty(name, primitiveSchema.toString());
        }

        return Wrapper.wrapProperty(name, buildSchema(0));
    }
}

