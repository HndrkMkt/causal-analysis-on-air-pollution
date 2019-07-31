package de.tuberlin.dima.bdapro.dataIntegration.weather;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

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
    void testEqualsAndHash(){
        WeatherReading reading1 = new WeatherReading();
        WeatherReading reading2 = new WeatherReading();

        // location
        assertHashAndEquality(reading1, reading2, true);
        reading1.location = "Mitte";
        assertHashAndEquality(reading1, reading2, false);
        reading2.location = "Kreuzberg";
        assertHashAndEquality(reading1, reading2, false);
        reading2.location = "Mitte";
        assertHashAndEquality(reading1, reading2, true);

        // time
        assertHashAndEquality(reading1, reading2, true);
        reading1.time = Timestamp.valueOf("2019-01-01 12:00:00");
        assertHashAndEquality(reading1, reading2, false);
        reading2.time = Timestamp.valueOf("2019-01-01 12:00:01");
        assertHashAndEquality(reading1, reading2, false);
        reading2.time = Timestamp.valueOf("2019-01-01 12:00:00");
        assertHashAndEquality(reading1, reading2, true);

        // longitude
        assertHashAndEquality(reading1, reading2, true);
        reading1.longitude = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.longitude = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.longitude = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // latitude
        assertHashAndEquality(reading1, reading2, true);
        reading1.latitude = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.latitude = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.latitude = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // temperature
        assertHashAndEquality(reading1, reading2, true);
        reading1.temperature = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.temperature = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.temperature = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // apparent_temperature
        assertHashAndEquality(reading1, reading2, true);
        reading1.apparent_temperature = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.apparent_temperature = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.apparent_temperature = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // cloud_cover
        assertHashAndEquality(reading1, reading2, true);
        reading1.cloud_cover = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.cloud_cover = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.cloud_cover = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // dew_point
        assertHashAndEquality(reading1, reading2, true);
        reading1.dew_point = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.dew_point = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.dew_point = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // humidity
        assertHashAndEquality(reading1, reading2, true);
        reading1.humidity = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.humidity = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.humidity = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // ozone
        assertHashAndEquality(reading1, reading2, true);
        reading1.ozone = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.ozone = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.ozone = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // precip_intensity
        assertHashAndEquality(reading1, reading2, true);
        reading1.precip_intensity = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.precip_intensity = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.precip_intensity = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // precip_probability
        assertHashAndEquality(reading1, reading2, true);
        reading1.precip_probability = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.precip_probability = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.precip_probability = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // precip_type
        assertHashAndEquality(reading1, reading2, true);
        reading1.precip_type = "snow";
        assertHashAndEquality(reading1, reading2, false);
        reading2.precip_type = "rain";
        assertHashAndEquality(reading1, reading2, false);
        reading2.precip_type = "snow";
        assertHashAndEquality(reading1, reading2, true);

        // pressure
        assertHashAndEquality(reading1, reading2, true);
        reading1.pressure = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.pressure = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.pressure = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // uv_index
        assertHashAndEquality(reading1, reading2, true);
        reading1.uv_index = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.uv_index = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.uv_index = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // visibility
        assertHashAndEquality(reading1, reading2, true);
        reading1.visibility = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.visibility = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.visibility = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // wind bearing
        assertHashAndEquality(reading1, reading2, true);
        reading1.wind_bearing = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.wind_bearing = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.wind_bearing = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // wind gust
        assertHashAndEquality(reading1, reading2, true);
        reading1.wind_gust = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.wind_gust = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.wind_gust = 13.2;
        assertHashAndEquality(reading1, reading2, true);

        // wind speed
        assertHashAndEquality(reading1, reading2, true);
        reading1.wind_speed = 13.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.wind_speed = 13.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.wind_speed = 13.2;
        assertHashAndEquality(reading1, reading2, true);
    }

    static FileInputSplit createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();

        try (Writer out = new OutputStreamWriter(new FileOutputStream(tempFile))) {
            out.write(contents);
        }

        return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[]{"localhost"});
    }

    private void assertHashAndEquality(WeatherReading reading1, WeatherReading reading2, boolean match){
        if (match) {
            Assertions.assertEquals(reading1, reading2);
            Assertions.assertEquals(reading1.hashCode(), reading2.hashCode());
        } else {
            Assertions.assertNotEquals(reading1, reading2);
            Assertions.assertNotEquals(reading1.hashCode(), reading2.hashCode());
        }
    }
}
