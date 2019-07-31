package de.tuberlin.dima.bdapro.dataIntegration.weather;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Test cases for the {@link WeatherReadingParser}
 *
 * @author Ricardo Salazar
 */

public class WeatherReadingParserTest {
    @Test
    void testReadRecordThrowsExceptionIfInputHasTooManyFields() throws IllegalArgumentException {
        WeatherReadingParser parser = new WeatherReadingParser(WeatherReading.getFields());
        Assertions.assertThrows(IndexOutOfBoundsException.class,
                () -> parser.readRecord("Berlin Mitte;2019-05-07 22:00:00;13.4049;52.52;4.63;4.63;0.0;-0.73;0.68;427.53;0.0;0.0;;1016.6;0.0;16.09;212.0;1.24;1.23;1\n"));

    }

    @Test
    void testReadRecord() throws Exception {
        WeatherReadingParser parser = new WeatherReadingParser(WeatherReading.getFields());
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
        Assertions.assertEquals(expected,
                parser.readRecord("Berlin Mitte;2019-05-07 22:00:00;13.4049;52.52;4.63;4.63;0.0;-0.73;0.68;427.53;0.0;0.0;;1016.6;0.0;16.09;212.0\n"));
        expected.wind_gust = 1.24;
        expected.wind_speed = 1.23;

        WeatherReading parsed = parser.readRecord("Berlin Mitte;2019-05-07 22:00:00;13.4049;52.52;4.63;4.63;0.0;-0.73;0.68;427.53;0.0;0.0;;1016.6;0.0;16.09;212.0;1.24;1.23\n");
        Assertions.assertEquals(expected, parsed);
    }
}
