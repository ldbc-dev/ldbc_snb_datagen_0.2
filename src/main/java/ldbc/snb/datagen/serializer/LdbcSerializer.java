package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.util.formatter.DateFormatter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Map;

abstract public class LdbcSerializer<TWriter extends HdfsWriter> implements Serializer<TWriter> {

    protected Map<FileName, TWriter> writers;

    private DateFormatter dateFormatter;

    abstract public List<FileName> getFileNames();

    abstract public void writeFileHeaders();

    protected void addition() {}

    protected abstract boolean isDynamic();

    public void initialize(FileSystem fs, String outputDir, int reducerId, double oversizeFactor, boolean isCompressed) throws IOException {
        writers = initialize(fs, outputDir, reducerId, oversizeFactor, isCompressed, isDynamic(), getFileNames());
        addition();
        writeFileHeaders();
        this.dateFormatter = new DateFormatter();
    }

    protected String formatDateTime(long epochMillis) {
        return dateFormatter.formatDateTime(epochMillis);
    }

    protected String formatDate(long epochMillis) {
        return dateFormatter.formatDate(epochMillis);
    }

    public void close() {
        for (FileName f : getFileNames()) {
            writers.get(f).close();
        }
    }
}