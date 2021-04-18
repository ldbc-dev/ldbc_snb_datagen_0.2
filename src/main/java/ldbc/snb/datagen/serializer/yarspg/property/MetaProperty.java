package ldbc.snb.datagen.serializer.yarspg.property;

class MetaProperty extends Property {
    MetaProperty(String metaValue) {
        super(metaValue);
    }

    @Override
    public String toString() {
        return "@" + super.getName() + ":" + super.toString();
    }
}