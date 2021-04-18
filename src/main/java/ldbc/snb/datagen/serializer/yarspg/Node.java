package ldbc.snb.datagen.serializer.yarspg;

import ldbc.snb.datagen.serializer.yarspg.property.Properties;
import ldbc.snb.datagen.serializer.yarspg.property.Property;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Node extends Statement {
    protected String nodeID;
    protected List<String> nodeLabel;
    private Properties properties;

    public Node(String nodeID) {
        this.nodeID = nodeID;
    }

    public Node withNodeLabels(String... labels) {
        this.nodeLabel = Arrays.asList(labels);

        return this;
    }

    public Node withProperties(Consumer<Properties> consumer) {
        properties = new Properties();
        consumer.accept(properties);

        return this;
    }

    public Map<String, Property> getProperties() {
        return properties.getProperties();
    }

    public String getNodeID() {
        return nodeID;
    }

    public List<String> getNodeLabel() {
        return nodeLabel;
    }

    public void withNodeID(String nodeId) {
        this.nodeID = nodeId;
    }

    @Override
    public String toString() {
        String props = properties != null ? properties.toString() : "";
        String wrappedNodeLabel = Wrapper.wrap(nodeLabel.stream()
                                                        .map(Wrapper::wrap)
                                                        .collect(Collectors.joining(", ")), Wrapper.Bracket.CURLY);

        return Wrapper.wrap("ID-" + nodeID + wrappedNodeLabel + props + graphsList + metadataList,
                            Wrapper.Bracket.ROUND);
    }

}
