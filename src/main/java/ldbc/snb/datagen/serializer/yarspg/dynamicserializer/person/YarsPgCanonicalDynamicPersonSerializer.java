package ldbc.snb.datagen.serializer.yarspg.dynamicserializer.person;

import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;

public class YarsPgCanonicalDynamicPersonSerializer extends YarsPgDynamicPersonSerializer {
    @Override
    protected void addition() {
        getFileNames().forEach(fileName -> writers.get(fileName).setCanonical(true));
    }

    @Override
    public void writeFileHeaders() {
        getFileNames().forEach(fileName -> writers.get(fileName).writeHeader(HdfsYarsPgWriter.CANONICAL_HEADERS));
    }
}
