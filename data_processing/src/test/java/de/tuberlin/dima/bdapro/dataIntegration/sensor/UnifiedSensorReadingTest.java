package de.tuberlin.dima.bdapro.dataIntegration.sensor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;


class UnifiedSensorReadingTest {
    @Test
    void testEqualsAndHash() {
        UnifiedSensorReading reading1 = new UnifiedSensorReading();
        UnifiedSensorReading reading2 = new UnifiedSensorReading();

        // sensorId
        assertHashAndEquality(reading1, reading2, true);
        reading1.sensorId = 1;
        assertHashAndEquality(reading1, reading2, false);
        reading2.sensorId = 2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.sensorId = 1;
        assertHashAndEquality(reading1, reading2, true);

        // sensorType
        reading1.sensorType = "foo";
        assertHashAndEquality(reading1, reading2, false);
        reading2.sensorType = "bar";
        assertHashAndEquality(reading1, reading2, false);
        reading2.sensorType = "foo";
        assertHashAndEquality(reading1, reading2, true);

        // location
        reading1.location = 2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.location = 3;
        assertHashAndEquality(reading1, reading2, false);
        reading2.location = 2;
        assertHashAndEquality(reading1, reading2, true);

        // lat
        reading1.lat = 3.4;
        assertHashAndEquality(reading1, reading2, false);
        reading2.lat = 4.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.lat = 3.4;
        assertHashAndEquality(reading1, reading2, true);

        // lon
        reading1.lon = 4.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.lon = 5.6;
        assertHashAndEquality(reading1, reading2, false);
        reading2.lon = 4.5;
        assertHashAndEquality(reading1, reading2, true);

        // timestamp
        reading1.timestamp = Timestamp.valueOf("2019-01-01 12:00:00");
        assertHashAndEquality(reading1, reading2, false);
        reading2.timestamp = Timestamp.valueOf("2019-01-01 12:00:01");
        assertHashAndEquality(reading1, reading2, false);
        reading2.timestamp = Timestamp.valueOf("2019-01-01 12:00:00");
        assertHashAndEquality(reading1, reading2, true);

        // pressure
        reading1.pressure = 5.6;
        assertHashAndEquality(reading1, reading2, false);
        reading2.pressure = 6.7;
        assertHashAndEquality(reading1, reading2, false);
        reading2.pressure = 5.6;
        assertHashAndEquality(reading1, reading2, true);

        // altitude
        reading1.altitude = 6.7;
        assertHashAndEquality(reading1, reading2, false);
        reading2.altitude = 7.8;
        assertHashAndEquality(reading1, reading2, false);
        reading2.altitude = 6.7;
        Assertions.assertEquals(reading1, reading2);

        // pressure_sealevel
        reading1.pressure_sealevel = 7.8;
        assertHashAndEquality(reading1, reading2, false);
        reading2.pressure_sealevel = 8.9;
        assertHashAndEquality(reading1, reading2, false);
        reading2.pressure_sealevel = 7.8;
        assertHashAndEquality(reading1, reading2, true);

        // temperature
        reading1.temperature = 9.0;
        assertHashAndEquality(reading1, reading2, false);
        reading2.temperature = 0.1;
        assertHashAndEquality(reading1, reading2, false);
        reading2.temperature = 9.0;
        assertHashAndEquality(reading1, reading2, true);

        // humidity
        reading1.humidity = 0.1;
        assertHashAndEquality(reading1, reading2, false);
        reading2.humidity = 1.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.humidity = 0.1;
        assertHashAndEquality(reading1, reading2, true);

        // p1
        reading1.p1 = 1.2;
        assertHashAndEquality(reading1, reading2, false);
        reading2.p1 = 2.3;
        assertHashAndEquality(reading1, reading2, false);
        reading2.p1 = 1.2;
        assertHashAndEquality(reading1, reading2, true);

        // p2
        reading1.p2 = 2.3;
        assertHashAndEquality(reading1, reading2, false);
        reading2.p2 = 3.4;
        assertHashAndEquality(reading1, reading2, false);
        reading2.p2 = 2.3;
        assertHashAndEquality(reading1, reading2, true);

        // p0
        reading1.p0 = 3.4;
        assertHashAndEquality(reading1, reading2, false);
        reading2.p0 = 4.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.p0 = 3.4;
        assertHashAndEquality(reading1, reading2, true);

        // durP1
        reading1.durP1 = 4.5;
        assertHashAndEquality(reading1, reading2, false);
        reading2.durP1 = 5.6;
        assertHashAndEquality(reading1, reading2, false);
        reading2.durP1 = 4.5;
        assertHashAndEquality(reading1, reading2, true);

        // ratioP1
        reading1.ratioP1 = 5.6;
        assertHashAndEquality(reading1, reading2, false);
        reading2.ratioP1 = 6.7;
        assertHashAndEquality(reading1, reading2, false);
        reading2.ratioP1 = 5.6;
        assertHashAndEquality(reading1, reading2, true);

        // durP2
        reading1.durP2 = 6.7;
        assertHashAndEquality(reading1, reading2, false);
        reading2.durP2 = 7.8;
        assertHashAndEquality(reading1, reading2, false);
        reading2.durP2 = 6.7;
        assertHashAndEquality(reading1, reading2, true);

        // ratioP2
        reading1.ratioP2 = 7.8;
        assertHashAndEquality(reading1, reading2, false);
        reading2.ratioP2 = 8.9;
        assertHashAndEquality(reading1, reading2, false);
        reading2.ratioP2 = 7.8;
        assertHashAndEquality(reading1, reading2, true);
    }

    @Test
    void testToFormatString() {
        UnifiedSensorReading reading = new UnifiedSensorReading();
        reading.sensorId = 1;
        reading.sensorType = "foo";
        reading.location = 2;
        reading.lat = 3.4;
        reading.lon = 4.5;
        reading.timestamp = Timestamp.valueOf("2019-01-01 12:00:00");
        reading.pressure = 5.6;
        reading.altitude = 6.7;
        reading.pressure_sealevel = 7.8;
        reading.temperature = 8.9;
        reading.humidity = 9.0;
        reading.p1 = 0.1;
        reading.p2 = 1.2;
        reading.p0 = 2.3;
        reading.durP1 = 3.4;
        reading.ratioP1 = 4.5;
        reading.durP2 = 5.6;
        reading.ratioP2 = 6.7;

        Assertions.assertEquals("1;foo;2;3.4;4.5;2019-01-01T12:00:00;5.6;6.7;7.8;8.9;9.0;0.1;1.2;2.3;3.4;4.5;5.6;6.7",
                reading.toString(Type.UNIFIED));
        Assertions.assertEquals("1;foo;2;3.4;4.5;2019-01-01T12:00:00;5.6;6.7;7.8;8.9;9.0",
                reading.toString(Type.BME280));
    }

    private void assertHashAndEquality(UnifiedSensorReading reading1, UnifiedSensorReading reading2, boolean match) {
        if (match) {
            Assertions.assertEquals(reading1, reading2);
            Assertions.assertEquals(reading1.hashCode(), reading2.hashCode());
        } else {
            Assertions.assertNotEquals(reading1, reading2);
            Assertions.assertNotEquals(reading1.hashCode(), reading2.hashCode());
        }
    }
}
