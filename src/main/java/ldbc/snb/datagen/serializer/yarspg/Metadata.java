package ldbc.snb.datagen.serializer.yarspg;

import java.util.ArrayList;
import java.util.List;

public class Metadata {
    List<String> metaList = new ArrayList<>();

    public void add(String key, String value) {
        metaList.add(Wrapper.wrap(Wrapper.wrap(key), Wrapper.wrap(value)));
    }

    public void add(String key, String value, boolean wrappedKey, boolean wrappedValue) {
        String wK = wrappedKey ? Wrapper.wrap(key) : key;
        String wV = wrappedValue ? Wrapper.wrap(value) : value;
        metaList.add(Wrapper.wrap(wK, wV));
    }

    @Override
    public String toString() {
        return "+" + metaList.toString();
    }
}
