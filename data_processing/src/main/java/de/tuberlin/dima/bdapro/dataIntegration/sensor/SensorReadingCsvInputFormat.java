package de.tuberlin.dima.bdapro.dataIntegration.sensor;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * This class extends the DelimitedInputFormat to recursively find all files matching a given filter
 * for a sensor type.
 */
public class SensorReadingCsvInputFormat extends DelimitedInputFormat<UnifiedSensorReading> {
    private static final long serialVersionUID = 1L;
    /**
     * The name of the charset to use for decoding.
     */
    private final static String CHARSET_NAME = "UTF-8";

    private final SensorReadingParser parser;


    /**
     * Creates a new SensorReadingCsvInputFormat for a given base file path and sensor type.
     *
     * @param filePath   The base path to start the search for matching input files
     * @param sensorType The sensor type to retrieve.
     */
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

    /**
     * {@inheritDoc}
     * <p>
     * Skips the first line of the CSV file that includes the column names.
     */
    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);

        if (this.splitStart == 0L) {
            this.readLine();
        }
    }


    /**
     * {@inheritDoc}
     * <p>
     * Recursively searches all directories except for hidden and magic directories. Applies the filter logic only to
     * files.
     */
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
