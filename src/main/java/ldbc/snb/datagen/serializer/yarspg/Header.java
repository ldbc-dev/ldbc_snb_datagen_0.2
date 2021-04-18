package ldbc.snb.datagen.serializer.yarspg;


public enum Header {
    METADATA("%METADATA"),
    GRAPH("%GRAPH"),
    NODE_SCHEMA("%NODE SCHEMAS"),
    EDGE_SCHEMA("%EDGE SCHEMAS"),
    NODES("%NODES"),
    EDGES("%EDGES");

    private final String name;

    Header(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

}