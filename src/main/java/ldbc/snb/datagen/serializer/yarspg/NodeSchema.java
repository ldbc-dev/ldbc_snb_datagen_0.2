package ldbc.snb.datagen.serializer.yarspg;

import ldbc.snb.datagen.serializer.yarspg.property.schema.SchemasOfProperty;

import java.util.function.Consumer;
import java.util.stream.Collectors;

public class NodeSchema extends Node {
    protected SchemasOfProperty schemasOfProperty;

    public NodeSchema(String nodeID) {
        super(nodeID);
    }

    public NodeSchema withPropsDefinition(Consumer<SchemasOfProperty> consumer) {
        schemasOfProperty = new SchemasOfProperty();
        consumer.accept(schemasOfProperty);
        return this;
    }

    @Override
    public NodeSchema withNodeLabels(String... labels) {
        return (NodeSchema) super.withNodeLabels(labels);
    }

    @Override
    public String toString() {
        String schemaNodeID = Integer.toString(Math.abs(nodeID.hashCode()));
        String schemaProps = schemasOfProperty != null ? schemasOfProperty.toString() : "";
        String wrappedNodeLabel = Wrapper.wrap(nodeLabel.stream()
                                                        .map(Wrapper::wrap)
                                                        .collect(Collectors.joining(", ")), Wrapper.Bracket.CURLY);

        return "S" + Wrapper.wrap("ID-" + schemaNodeID + wrappedNodeLabel + schemaProps + graphsList + metadataList,
                                  Wrapper.Bracket.ROUND);
    }
}
