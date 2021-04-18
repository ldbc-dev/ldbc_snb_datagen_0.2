package ldbc.snb.datagen.serializer.yarspg.property.schema;

import ldbc.snb.datagen.serializer.yarspg.property.ComplexType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ComplexSchema {
    private final ComplexType complexType;
    private MetaPropertySchema metaSchema;
    private String optional = "";
    private String nullable = "";
    private String max = "", min = "";

    ComplexSchema(ComplexType complexType) {
        this.complexType = complexType;
    }

    public ComplexSchema asMax(Integer max) {
        this.max = max.toString();
        return this;
    }

    public ComplexSchema asMin(Integer min) {
        this.min = min.toString();
        return this;
    }

    public ComplexSchema asOptional() {
        this.optional = "OPTIONAL";
        return this;
    }

    public ComplexSchema asNullable() {
        this.nullable = "NULL";
        return this;
    }

    public ComplexSchema withMeta(String metaValue, Consumer<MetaPropertySchema> consumer) {
        metaSchema = new MetaPropertySchema(metaValue);
        consumer.accept(metaSchema);
        return this;
    }

    public MetaPropertySchema getMetaSchema() {
        return metaSchema;
    }

    public ComplexType getComplexType() {
        return complexType;
    }

    public String getOptional() {
        return optional;
    }

    public String getNullable() {
        return nullable;
    }

    public String getMax() {
        return max;
    }

    public String getMin() {
        return min;
    }

    @Override
    public String toString() {
        List<String> definition = new ArrayList<String>() {{
            add(optional);
            add(nullable);
            if (!max.isEmpty()) add("MAX " + max);
            if (!min.isEmpty()) add("MIN " + min);
        }};

        return definition.stream()
                         .filter(el -> !el.isEmpty())
                         .collect(Collectors.joining(" "));
    }
}

