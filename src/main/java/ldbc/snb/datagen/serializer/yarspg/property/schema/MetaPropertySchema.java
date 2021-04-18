package ldbc.snb.datagen.serializer.yarspg.property.schema;

public class MetaPropertySchema extends PropertySchema {
    MetaPropertySchema(String metaValue) {
        super(metaValue);
    }

    @Override
    public String toString() {
        return "@" + super.toString();
    }
}

