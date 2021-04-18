package ldbc.snb.datagen.serializer.yarspg;


import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.Serializer;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface YarsPgSerializer extends Serializer<HdfsYarsPgWriter> {
    String VERSION = "1.0";

    default Map<FileName, HdfsYarsPgWriter> initialize(FileSystem fs, String outputDir, int reducerId, double oversizeFactor, boolean isCompressed, boolean dynamic,
                                                       List<FileName> fileNames) throws IOException {
        Map<FileName, HdfsYarsPgWriter> writers = new HashMap<>();
        for (FileName f : fileNames) {
            HdfsYarsPgWriter w = new HdfsYarsPgWriter(
                    fs,
                    outputDir + "/yarspg/raw/composite-merged-fk" + (dynamic ? "/dynamic/" : "/static/") + f.name + "/",
                    String.valueOf(reducerId),
                    (int) Math.ceil(f.size / oversizeFactor),
                    isCompressed
            );
            writers.put(f, w);
        }

        return writers;
    }

    default void standardHeaders(String x) {

    }
}
