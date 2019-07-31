package de.tuberlin.dima.bdapro.dataIntegration.weather;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * This class contains the logic to format, read and parse the csv weather input.
 *
 * @author Ricardo Salazar
 */
public class WeatherReadingInputFormat extends DelimitedInputFormat<WeatherReading> {
    private static final long serialVersionUID = 1L;

    private final WeatherReadingParser parser;

    /**
     * Creates a new WeatherReadingInputFormat instance
     *
     * @param filePath a valid path for the weather input data
     * @param parser a {@link WeatherReadingParser} containing the corresponding valid fields of the {@link WeatherReading} class.
     */
    public WeatherReadingInputFormat(Path filePath, WeatherReadingParser parser) {
        super(filePath, null);
        this.parser = parser;
    }

    @Override
    public WeatherReading readRecord(WeatherReading reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        /**
         * The name of the charset to use for decoding.
         */
        String charsetName = "UTF-8";
        String line = new String(bytes, offset, numBytes, charsetName);
        try {
            return parser.readRecord(line);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        /**
         * Skips the first line of the CSV file that includes the column names.
         */
        super.open(split);

        if (this.splitStart == 0L) {
            this.readLine();
        }
    }
}
