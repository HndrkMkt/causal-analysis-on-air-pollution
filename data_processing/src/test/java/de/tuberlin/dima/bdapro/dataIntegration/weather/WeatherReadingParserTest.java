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
}
