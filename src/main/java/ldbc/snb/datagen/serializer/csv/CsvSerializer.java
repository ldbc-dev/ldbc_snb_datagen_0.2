package ldbc.snb.datagen.serializer.csv;

import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.Serializer;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface CsvSerializer extends Serializer<HdfsCsvWriter> {

    default Map<FileName, HdfsCsvWriter> initialize(FileSystem fs,
                                                    String outputDir,
                                                    int reducerId,
                                                    double oversizeFactor,
                                                    boolean isCompressed,
                                                    boolean dynamic,
                                                    List<FileName> fileNames) throws IOException {
        Map<FileName, HdfsCsvWriter> writers = new HashMap<>();

        for (FileName f : fileNames) {
            writers.put(f, new HdfsCsvWriter(
                            fs,
                            outputDir + "/csv/raw/composite-merged-fk" + (dynamic ? "/dynamic/" : "/static/") + f.name + "/",
                            String.valueOf(reducerId),
                            (int) Math.ceil(f.size / oversizeFactor),
                            isCompressed,
                            "|"
                    )
            );
        }
        return writers;
    }

}
