package ldbc.snb.datagen.serializer.yarspg;


import ldbc.snb.datagen.serializer.yarspg.property.Properties;
import ldbc.snb.datagen.serializer.yarspg.property.Property;

import java.util.Map;
import java.util.function.Consumer;

public class Edge extends Statement {
    protected String edgeID = "";
    protected EdgeType edgeType;
    protected String edgeLabel;
    private Properties properties;
    protected String leftNodeID = "";
    protected String rightNodeID = "";

    public Edge(EdgeType edgeType) {
        this.edgeType = edgeType;
    }

    public Edge(String leftNodeID, EdgeType edgeType, String rightNodeID) {
        this.leftNodeID = leftNodeID;
        this.edgeType = edgeType;
        this.rightNodeID = rightNodeID;
    }

    public Edge as(String leftNodeID, String edgeLabel, String rightNodeID) {
        this.leftNodeID = leftNodeID;
        this.edgeLabel = edgeLabel;
        this.rightNodeID = rightNodeID;
        return this;
    }

    public Edge withEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
        return this;
    }

    public Edge withEdgeID(String edgeID) {
        this.edgeID = edgeID;
        return this;
    }

    public Edge withLeftEdgeID(String leftNodeID) {
        this.leftNodeID = leftNodeID;
        return this;
    }

    public Edge withRightEdgeID(String rightNodeID) {
        this.rightNodeID = rightNodeID;
        return this;
    }

    public Edge withProperties(Consumer<Properties> consumer) {
        properties = new Properties();
        consumer.accept(properties);

        return this;
    }

    public String getLeftNodeID() {
        return leftNodeID;
    }

    public String getRightNodeID() {
        return rightNodeID;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public Map<String, Property> getProperties() {
        return properties.getProperties();
    }

    public static String leftDirection(EdgeType edgeType) {
        if (edgeType == EdgeType.DIRECTED) return "-";
        if (edgeType == EdgeType.BIDIRECTED) return "<-";
        return "-";
    }

    public static String rightDirection(EdgeType edgeType) {
        if (edgeType == EdgeType.DIRECTED) return "->";
        if (edgeType == EdgeType.BIDIRECTED) return "->";
        return "-";
    }

    @Override
    public String toString() {
        String leftEdge = leftDirection(edgeType);
        String rightEdge = rightDirection(edgeType);
        String props = properties != null ? properties.toString() : "";
        String wrappedEdgeLabel = Wrapper.wrapWithQuote(edgeLabel, Wrapper.Bracket.CURLY);

        return Wrapper.wrap("ID-" + leftNodeID, Wrapper.Bracket.ROUND)
                + leftEdge
                + Wrapper.wrap(edgeID + wrappedEdgeLabel + props, Wrapper.Bracket.ROUND)
                + rightEdge
                + Wrapper.wrap("ID-" + rightNodeID, Wrapper.Bracket.ROUND) + graphsList + metadataList;
    }
}
