package ldbc.snb.datagen.serializer.yarspg.staticserializer;

import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;

public class YarsPgCanonicalSchemalessStaticSerializer extends YarsPgSchemalessStaticSerializer {
    @Override
    protected void addition() {
        getFileNames().forEach(fileName -> writers.get(fileName).setCanonical(true));
    }

    @Override
    public void writeFileHeaders() {
        getFileNames().forEach(fileName -> writers.get(fileName).writeHeader(HdfsYarsPgWriter.CANONICAL_HEADERS));
    }
}
