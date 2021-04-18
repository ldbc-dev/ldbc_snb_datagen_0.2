package ldbc.snb.datagen.hadoop.writer;

import ldbc.snb.datagen.serializer.yarspg.*;
import ldbc.snb.datagen.vocabulary.DC;
import ldbc.snb.datagen.vocabulary.OWL;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class HdfsYarsPgWriter extends HdfsWriter {
    private final StringBuffer buffer;
    private boolean isCanonical = false;

    private Node node;
    private NodeSchema nodeSchema;
    private Edge edge;
    private EdgeSchema edgeSchema;

    private String footer = "";
    private HashSet<String> edgeSchemaKey = new HashSet<>();
    private HashSet<String> edgeKey = new HashSet<>();
    private HashSet<String> nodeSchemaKey = new HashSet<>();
    private HashSet<String> nodeKey = new HashSet<>();

    public static final String STANDARD_HEADERS = DC.PREFIX_DECLARATION + "\n" +
            OWL.PREFIX_DECLARATION + "\n" +
            "-" + DC.prefixed("creator:") + "\"LDBC\"" + "\n" +
            "-" + DC.prefixed("created:") + "\"" + new Date().toString() + "\"" + "\n" +
            "-" + DC.prefixed("title:") + "\"" + "Social Network Benchmark" + "\"" + "\n" +
            "-" + DC.prefixed("identifier:") + "\"" + UUID.randomUUID() + "\"" + "\n" +
            "-" + OWL.prefixed("version:") + "\"" + YarsPgSerializer.VERSION + "\"";

    public static final String CANONICAL_HEADERS = "-" + DC.fullprefixed("creator") + ":\"LDBC\"" +
            "-" + DC.fullprefixed("created") + ":\"" + new Date().toString() + "\"" + "\n" +
            "-" + DC.fullprefixed("title") + ":\"" + "Social Network Benchmark" + "\"" + "\n" +
            "-" + DC.fullprefixed("identifier") + ":\"" + UUID.randomUUID() + "\"" + "\n" +
            "-" + OWL.fullprefixed("version") + ":\"" + YarsPgSerializer.VERSION + "\"";


    public HdfsYarsPgWriter(FileSystem fs, String outputDir, String prefix, int numFiles, boolean compressed) throws IOException {
        super(fs, outputDir, prefix, numFiles, compressed, "yarspg");
        this.buffer = new StringBuffer();
    }

    public void setCanonical(boolean canonical) {
        isCanonical = canonical;
    }

    public boolean getCanonical() {
        return isCanonical;
    }

    @Override
    public void close() {
        if (!isCanonical) {
            this.write("# End of " + footer + "\n");
        }

        super.close();
    }

    public void writeHeader(String header) {
        buffer.setLength(0);
        buffer.append(header)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeNode(String nodeID, Consumer<Node> consumer) {
        buffer.setLength(0);
        node = new Node(nodeID);
        consumer.accept(node);

        buffer.append(node)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeEdge(EdgeType edgeType, Consumer<Edge> edgeConsumer) {
        buffer.setLength(0);
        edge = new Edge(edgeType);
        edgeConsumer.accept(edge);

        buffer.append(edge)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeNode(String schemaNodeID, String nodeID, BiConsumer<NodeSchema, Node> biConsumer) {
        buffer.setLength(0);

        nodeSchema = new NodeSchema(schemaNodeID);
        node = new Node(nodeID);
        biConsumer.accept(nodeSchema, node);

        // Make sure that schema and section is written only once
        // Workaround to `PersonExporter` implementation
        if (!nodeSchemaKey.contains(nodeSchema.getNodeLabel()
                .get(0))) {
            footer = nodeSchema.getNodeLabel().toString();

            if (isCanonical) {
                buffer.append(Header.NODE_SCHEMA.toString())
                        .append("\n");
            }
            buffer.append(nodeSchema)
                    .append("\n");
        }
        nodeSchemaKey.add(nodeSchema.getNodeLabel()
                .get(0));

        // Make sure that node section is written only once
        // Workaround to `PersonExporter` implementation
        if (isCanonical && !nodeKey.contains(node.getNodeLabel()
                .get(0))) {
            buffer.append(Header.NODES.toString())
                    .append("\n");
        }
        nodeKey.add(node.getNodeLabel()
                .get(0));


        buffer.append(node)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeEdge(EdgeType edgeType, BiConsumer<EdgeSchema, Edge> biConsumer) {
        buffer.setLength(0);

        edge = new Edge(edgeType);
        edgeSchema = new EdgeSchema(edgeType);
        biConsumer.accept(edgeSchema, edge);

        // Make sure that schema and section is written only once
        // Workaround to `PersonExporter` implementation
        if (!edgeSchemaKey.contains(edgeSchema.getEdgeLabel())) {
            footer = edgeSchema.getEdgeLabel();

            if (isCanonical) {
                buffer.append(Header.EDGE_SCHEMA.toString())
                        .append("\n");
            }

            buffer.append(edgeSchema)
                    .append("\n");

        }
        edgeSchemaKey.add(edgeSchema.getEdgeLabel());

        // Make sure that edge section is written only once
        // Workaround to `PersonExporter` implementation
        if (isCanonical && !edgeKey.contains(edge.getEdgeLabel())) {
            buffer.append(Header.EDGES.toString())
                    .append("\n");
        }
        edgeKey.add(edge.getEdgeLabel());


        buffer.append(edge)
                .append("\n");

        this.write(buffer.toString());
    }
}
