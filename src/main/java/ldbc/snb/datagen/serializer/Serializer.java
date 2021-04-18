package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Serializer<THDFSWriter extends HdfsWriter> {
    Map<FileName, THDFSWriter> initialize(
            FileSystem fs,
            String outputDir,
            int reducerId,
            double oversizeFactor,
            boolean isCompressed,
            boolean dynamic,
            List<FileName> fileNames
    ) throws IOException;
}
