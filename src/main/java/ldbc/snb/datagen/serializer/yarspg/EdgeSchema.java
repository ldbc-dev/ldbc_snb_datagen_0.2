package ldbc.snb.datagen.serializer.yarspg;

import ldbc.snb.datagen.serializer.yarspg.property.schema.SchemasOfProperty;

import java.util.function.Consumer;

public class EdgeSchema extends Edge {
    protected SchemasOfProperty schemasOfProperty;

    public EdgeSchema(EdgeType edgeType) {
        super(edgeType);
    }

    public EdgeSchema(String leftNodeID, EdgeType edgeType, String rightNodeID) {
        super(leftNodeID, edgeType, rightNodeID);
    }

    @Override
    public EdgeSchema as(String leftNodeID, String edgeLabel, String rightNodeID) {
        return (EdgeSchema) super.as(leftNodeID, edgeLabel, rightNodeID);
    }

    public String getEdgeID() {
        return edgeID;
    }

    public EdgeSchema withPropsDefinition(Consumer<SchemasOfProperty> consumer) {
        schemasOfProperty = new SchemasOfProperty();
        consumer.accept(schemasOfProperty);

        return this;
    }

    @Override
    public String toString() {
        String schemaLeftNodeID = Integer.toString(Math.abs(leftNodeID.hashCode()));
        String schemaRightNodeID = Integer.toString(Math.abs(rightNodeID.hashCode()));
        String leftEdge = Edge.leftDirection(edgeType);
        String rightEdge = Edge.rightDirection(edgeType);
        String schemaProps = schemasOfProperty != null ? schemasOfProperty.toString() : "";
        String wrappedEdgeLabel = Wrapper.wrapWithQuote(edgeLabel, Wrapper.Bracket.CURLY);

        return "S" + Wrapper.wrap("ID-" + schemaLeftNodeID, Wrapper.Bracket.ROUND)
                + leftEdge
                + Wrapper.wrap(edgeID + wrappedEdgeLabel + schemaProps, Wrapper.Bracket.ROUND)
                + rightEdge
                + Wrapper.wrap("ID-" + schemaRightNodeID, Wrapper.Bracket.ROUND) + graphsList + metadataList;
    }
}
