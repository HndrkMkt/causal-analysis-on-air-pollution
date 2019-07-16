package de.tuberlin.dima.bdapro.weather;

import de.tuberlin.dima.bdapro.parsers.WeatherReadingParser;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

public class WeatherReadingInputFormat extends DelimitedInputFormat<WeatherReading> {
    private static final long serialVersionUID = 1L;
    /**
     * The name of the charset to use for decoding.
     */
    private String charsetName = "UTF-8";

    private WeatherReadingParser parser;


    public WeatherReadingInputFormat(Path filePath, WeatherReadingParser parser) {
        super(filePath, null);
        this.parser = parser;
    }

    @Override
    public WeatherReading readRecord(WeatherReading reuse, byte[] bytes, int offset, int numBytes) throws IOException {
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
}
