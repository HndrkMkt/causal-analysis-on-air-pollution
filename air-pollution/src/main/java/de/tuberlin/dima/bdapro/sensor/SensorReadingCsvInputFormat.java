package de.tuberlin.dima.bdapro.sensor;

import de.tuberlin.dima.bdapro.parsers.SensorReadingParser;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * TODO: Comment EVERYTHING
 */
public class SensorReadingCsvInputFormat extends DelimitedInputFormat<UnifiedSensorReading> {
    private static final long serialVersionUID = 1L;
    /**
     * The name of the charset to use for decoding.
     */
    private final static String CHARSET_NAME = "UTF-8";

    private final SensorReadingParser parser;


    public SensorReadingCsvInputFormat(Path filePath, Type sensorType) {
        super(filePath, null);
        this.parser = new SensorReadingParser(sensorType);
    }

    @Override
    public UnifiedSensorReading readRecord(UnifiedSensorReading reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        String line = new String(bytes, offset, numBytes, CHARSET_NAME);
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
