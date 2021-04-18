package ldbc.snb.datagen.serializer.yarspg.property;

public enum PrimitiveType {

    DECIMAL("Decimal"),
    SMALL_INT("SmallInt"),
    INTEGER("Integer"),
    BIG_INT("BigInt"),
    FLOAT("Float"),
    REAL("Real"),
    DOUBLE("Double"),
    BOOL("Bool"),
    NULL("Null"),
    STRING("String"),
    DATE("Date"),
    DATE_TIME("DateTime"),
    TIME("Time"),
    ;

    private final String name;

    PrimitiveType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

}

