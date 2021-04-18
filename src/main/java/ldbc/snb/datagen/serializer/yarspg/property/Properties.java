package ldbc.snb.datagen.serializer.yarspg.property;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Data structure which holds properties of a node or edge
 */
public class Properties {
    private Map<String, Property> properties = new HashMap<>();
    private Property property;

    public Map<String, Property> getProperties() {
        return properties;
    }

    public Property getProperty(String key) {
        return properties.get(key);
    }

    public Map<String, Property> add(String propName, Consumer<Property> consumer) {
        property = new Property(propName);
        consumer.accept(property);
        properties.put(propName, property);
        return properties;
    }

    @Override
    public String toString() {
        return Arrays.toString(properties.values()
                                         .toArray());
    }
}
