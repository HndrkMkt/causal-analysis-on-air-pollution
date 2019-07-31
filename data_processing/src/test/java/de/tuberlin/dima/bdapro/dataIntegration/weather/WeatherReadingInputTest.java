package de.tuberlin.dima.bdapro.dataIntegration.weather;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;

/**
 * Test cases for the {@link WeatherReadingInputFormat}
 *
 * @author Ricardo Salazar
 */
public class WeatherReadingInputTest {

    @Test
    void testSkipFirstLine() throws IOException {
        WeatherReadingParser parser = new WeatherReadingParser(WeatherReading.getFields());
        WeatherReadingInputFormat format = new WeatherReadingInputFormat(new Path("file:///some/file/that/will/not/be/read"),parser);

        final String myString = "my mocked line 1\nmy mocked line 2\n";
        final FileInputSplit split = createTempFile(myString);
        format.open(split);
        Assertions.assertEquals(new Long(17), format.getCurrentState());
    }

    @Test
    void testReadRecord() throws IOException{
        ArrayList<Field> fields = new ArrayList<>();
        WeatherReadingParser parser = new WeatherReadingParser(WeatherReading.getFields());
        WeatherReadingInputFormat format = new WeatherReadingInputFormat(new Path("file:///some/file/that/will/not/be/read"),parser);

        final String weatherString = "location;time;longitude;latitude;temperature;apparent_temperature;cloud_cover;dew_point;humidity;ozone;precip_intensity;\n" +
                                        "precip_probability;precip_type;pressure;uv_index;visibility;wind_bearing;wind_gust;wind_speed\n" +
                                    "Berlin Mitte;2019-05-07 22:00:00;13.4049;52.52;4.63;4.63;0.0;-0.73;0.68;427.53;0.0;0.0;;1016.6;0.0;16.09;212.0;1.24;1.23";

        final FileInputSplit split = createTempFile(weatherString);
        format.setBufferSize(100);
        format.open(split);
        WeatherReading expected = new WeatherReading();
        expected.location = "Berlin Mitte";
        expected.time = Timestamp.valueOf("2019-05-07 22:00:00");
        expected.longitude = 13.4049;
        expected.latitude = 52.52;
        expected.temperature = 4.63;
        expected.apparent_temperature = 0.0;
        expected.cloud_cover = -0.73;
        expected.dew_point = 0.68;
        expected.humidity = 427.53;
        expected.ozone = 0.0;
        expected.precip_intensity = 0.0;
        expected.precip_probability = 0.2;
        expected.precip_type = "";
        expected.pressure = 1016.6;
        expected.uv_index = 0.0;
        expected.visibility = 16.09;
        expected.wind_bearing = 212.0;
        expected.wind_gust = 1.24;
        expected.wind_speed = 1.23;

        WeatherReading first = format.nextRecord(new WeatherReading());
        Assertions.assertEquals(expected, first);

    }

    static FileInputSplit createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();

        try (Writer out = new OutputStreamWriter(new FileOutputStream(tempFile))) {
            out.write(contents);
        }

        return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[]{"localhost"});
    }
}
