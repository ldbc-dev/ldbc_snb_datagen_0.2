package ldbc.snb.datagen.serializer.yarspg.dynamicserializer.activity;


import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;

public class YarsPgCanonicalSchemalessDynamicActivitySerializer extends YarsPgSchemalessDynamicActivitySerializer {
    @Override
    protected void addition() {
        getFileNames().forEach(fileName -> writers.get(fileName).setCanonical(true));
    }

    @Override
    public void writeFileHeaders() {
        getFileNames().forEach(fileName -> writers.get(fileName).writeHeader(HdfsYarsPgWriter.CANONICAL_HEADERS));
    }
}

