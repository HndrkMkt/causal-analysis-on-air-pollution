package de.tuberlin.dima.bdapro;

import de.tuberlin.dima.bdapro.data.SensorReading;
import de.tuberlin.dima.bdapro.parsers.SensorReadingParser;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

public class NullableCsvInputFormat<T extends SensorReading> extends DelimitedInputFormat<T> {
    private static final long serialVersionUID = 1L;
    /**
     * The name of the charset to use for decoding.
     */
    private String charsetName = "UTF-8";

    private SensorReadingParser<T> parser;


    public NullableCsvInputFormat(Path filePath, SensorReadingParser<T> parser) {
        super(filePath, null);
        this.parser = parser;
    }

    @Override
    public T readRecord(T reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        String line = new String(bytes, offset, numBytes, charsetName);
        try {
            return parser.readRecord(line);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);

        if (this.splitStart == 0L) {
            this.readLine();
        }
    }

    @Override
    public boolean acceptFile(FileStatus fileStatus) {
        if (fileStatus.isDir() && enumerateNestedFiles) {
            String name = fileStatus.getPath().getName();
            return !name.startsWith("_") && !name.startsWith(".");
        } else {
            return super.acceptFile(fileStatus);
        }
    }
}
