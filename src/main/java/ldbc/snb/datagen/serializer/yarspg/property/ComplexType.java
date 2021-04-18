package ldbc.snb.datagen.serializer.yarspg.property;

public enum ComplexType {

    SET("Set"),
    LIST("List"),
    STRUCT("Struct"),
    ;

    private final String name;

    ComplexType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

}

