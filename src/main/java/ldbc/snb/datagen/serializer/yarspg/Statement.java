package ldbc.snb.datagen.serializer.yarspg;

import ldbc.snb.datagen.serializer.yarspg.property.Properties;
import ldbc.snb.datagen.serializer.yarspg.property.Property;

import java.util.Map;
import java.util.function.Consumer;

public abstract class Statement {
    protected String graphsList = "";
    protected String metadataList = "";

    public abstract Map<String, Property> getProperties();

    public abstract Statement withProperties(Consumer<Properties> consumer);

    /**
     * Generate an identifier for nodes and edges.
     * Represents bonding between nodes and edges, therefore as an argument
     * should take repeatable value f.e. message.id with additional string like serialization name.
     *
     * @param  toHashPayload  specify element that could be represented as string and converted to hashcode
     */
    public static <T> String generateId(T toHashPayload) {
        return String.valueOf(Math.abs(String.valueOf(toHashPayload)
                                             .hashCode()));
    }

    public String getGraphsList() {
        return graphsList;
    }

    public void setGraphsList(String graphsList) {
        this.graphsList = graphsList;
    }

    public String getMetadataList() {
        return metadataList;
    }

    public void setMetadataList(String metadataList) {
        this.metadataList = metadataList;
    }

    public Statement withGraphsList(String... graphsList) {
        this.graphsList = "/" + String.join(", ", graphsList) + "/";
        return this;
    }

    public Statement withLocalMetadataList(Consumer<Metadata> pairConsumer) {
        Metadata localeMetadata = new Metadata();
        pairConsumer.accept(localeMetadata);
        this.metadataList = localeMetadata.toString();
        return this;
    }
}

