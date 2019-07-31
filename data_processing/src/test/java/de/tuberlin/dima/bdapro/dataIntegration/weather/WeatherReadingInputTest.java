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
        reading1.location = "Mitte";
        reading2.location = "Kreuzberg";
        Assertions.assertNotEquals(reading1.location,reading2.location);
        reading1.location = "Mitte";
        reading2.location = "Mitte";
        Assertions.assertEquals(reading1.location,reading2.location);

        // time
        assertHashAndEquality(reading1, reading2, true);
        reading1.time = Timestamp.valueOf("2019-01-01 12:00:00");
        assertHashAndEquality(reading1, reading2, false);
        reading2.time = Timestamp.valueOf("2019-01-01 12:00:01");
        assertHashAndEquality(reading1, reading2, false);
        reading2.time = Timestamp.valueOf("2019-01-01 12:00:00");
        assertHashAndEquality(reading1, reading2, true);

        // longitude
        /*assertHashAndEquality(reading1, reading2, true);
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
        assertHashAndEquality(reading1, reading2, true);*/
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
