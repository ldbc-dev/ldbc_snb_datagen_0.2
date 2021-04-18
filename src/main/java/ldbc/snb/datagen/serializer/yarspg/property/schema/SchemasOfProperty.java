package ldbc.snb.datagen.serializer.yarspg.property.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class SchemasOfProperty {
    private final Map<String, PropertySchema> schemas = new HashMap<>();

    public Map<String, PropertySchema> getSchemas() {
        return schemas;
    }

    public PropertySchema getSchema(String key) {
        return schemas.get(key);
    }

    public Map<String, PropertySchema> add(String propName, Consumer<PropertySchema> consumer) {
        PropertySchema schema = new PropertySchema(propName);
        consumer.accept(schema);
        schemas.put(propName, schema);
        return schemas;
    }

    @Override
    public String toString() {
        return Arrays.toString(schemas.values()
                                      .toArray());
    }
}
