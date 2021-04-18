package ldbc.snb.datagen.serializer.yarspg.property.schema;

import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PrimitiveSchema {
    private final PrimitiveType primitiveType;
    private MetaPropertySchema metaSchema;

    private String optional = "";
    private String uniqueOrNull = "";

    public PrimitiveType getPrimitiveType() {
        return primitiveType;
    }

    PrimitiveSchema(PrimitiveType primitiveType) {
        this.primitiveType = primitiveType;
    }

    public String getOptional() {
        return optional;
    }

    public String getUniqueOrNull() {
        return uniqueOrNull;
    }

    public PrimitiveSchema asOptional() {
        this.optional = "OPTIONAL";
        return this;
    }

    public PrimitiveSchema asNullable() {
        this.uniqueOrNull = "NULL";
        return this;
    }

    public PrimitiveSchema asUnique() {
        this.uniqueOrNull = "UNIQUE";
        return this;
    }

    public MetaPropertySchema getMetaSchema() {
        return metaSchema;
    }

    public PrimitiveSchema withMeta(String metaValue, Consumer<MetaPropertySchema> consumer) {
        metaSchema = new MetaPropertySchema(metaValue);
        consumer.accept(metaSchema);
        return this;
    }

    @Override
    public String toString() {
        List<String> definition = new ArrayList<String>() {{
            add(primitiveType.toString());
            add(uniqueOrNull);
            add(optional);
            if (metaSchema != null) add(metaSchema.toString());
        }};

        return definition.stream()
                         .filter(el -> !el.isEmpty())
                         .collect(Collectors.joining(" "));
    }
}