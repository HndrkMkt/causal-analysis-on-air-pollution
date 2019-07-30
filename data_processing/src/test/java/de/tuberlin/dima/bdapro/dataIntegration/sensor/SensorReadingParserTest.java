package de.tuberlin.dima.bdapro.dataIntegration.sensor;

import de.tuberlin.dima.bdapro.dataIntegration.sensor.SensorReadingParser;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.Type;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.UnifiedSensorReading;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

/**
 * Test cases for the {@link SensorReadingParser}
 *
 * @author Hendrik Makait
 */
class SensorReadingParserTest {
    @Test
    void testReadRecordThrowsExceptionIfInputHasTooManyFields() throws IllegalArgumentException {
        SensorReadingParser parser = new SensorReadingParser(Type.BME280);
        Assertions.assertThrows(IndexOutOfBoundsException.class,
                () -> parser.readRecord("21847;BME280;11086;52.728;5.760;2019-04-01T00:00:00;102775.66;1.0;2.1;8.51;53.47;1\n"));
    }

    @Test
    void testReadRecordBME280() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.BME280);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 21847;
        expected.sensorType = "BME280";
        expected.location = 11086;
        expected.lat = 52.728;
        expected.lon = 5.760;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:00");
        expected.pressure = 102775.66;
        expected.temperature = 8.51;
        expected.humidity = 53.47;
        Assertions.assertEquals(expected,
                parser.readRecord("21847;BME280;11086;52.728;5.760;2019-04-01T00:00:00;102775.66;;;8.51;53.47\n"));
        expected.altitude = 1.0;
        expected.pressure_sealevel = 2.1;

        UnifiedSensorReading parsed = parser.readRecord("21847;BME280;11086;52.728;5.760;2019-04-01T00:00:00;102775.66;1.0;2.1;8.51;53.47\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.BME280)));

    }

    @Test
    void testReadRecordBMP180() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.BMP180);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 15845;
        expected.sensorType = "BMP180";
        expected.location = 8022;
        expected.lat = 52.498;
        expected.lon = 13.465;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:00");
        expected.pressure = 102401.00;
        expected.temperature = 10.40;

        Assertions.assertEquals(expected,
                parser.readRecord("15845;BMP180;8022;52.498;13.465;2019-04-01T00:00:00;102401.00;;;10.40\n"));
        expected.altitude = 1.0;
        expected.pressure_sealevel = 2.1;
        UnifiedSensorReading parsed = parser.readRecord("15845;BMP180;8022;52.498;13.465;2019-04-01T00:00:00;102401.00;1.0;2.1;10.40\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.BMP180)));
    }

    @Test
    void testReadRecordDHT22() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.DHT22);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 20638;
        expected.sensorType = "DHT22";
        expected.location = 10478;
        expected.lat = 49.004;
        expected.lon = 8.471;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:00");
        expected.temperature = 10.30;
        expected.humidity = 71.90;
        UnifiedSensorReading parsed = parser.readRecord("20638;DHT22;10478;49.004;8.471;2019-04-01T00:00:00;10.30;71.90\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.DHT22)));
    }

    @Test
    void testReadRecordDS18B20() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.DS18B20);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 8020;
        expected.sensorType = "DS18B20";
        expected.location = 4054;
        expected.lat = 52.666;
        expected.lon = 4.771;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:28");
        expected.temperature = 3.31;
        UnifiedSensorReading parsed = parser.readRecord("8020;DS18B20;4054;52.666;4.771;2019-04-01T00:00:28;3.31\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.DS18B20)));
    }

    @Test
    void testReadRecordHPM() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.HPM);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 7096;
        expected.sensorType = "HPM";
        expected.location = 3590;
        expected.lat = 50.865;
        expected.lon = 4.376;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:02:23");
        expected.p1 = 6.;
        expected.p2 = 5.;

        Assertions.assertEquals(expected,
                parser.readRecord("7096;HPM;3590;50.865;4.376;2019-04-01T00:02:23;6;5\n"));
    }

    @Test
    void testReadRecordHTU21D() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.HTU21D);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 19340;
        expected.sensorType = "HTU21D";
        expected.location = 9804;
        expected.lat = 52.349;
        expected.lon = 6.685;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:13");
        expected.temperature = 2.79;
        expected.humidity = 75.27;
        UnifiedSensorReading parsed = parser.readRecord("19340;HTU21D;9804;52.349;6.685;2019-04-01T00:00:13;2.79;75.27\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.HTU21D)));
    }

    @Test
    void testReadRecordPMS3003() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.PMS3003);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 397;
        expected.sensorType = "PMS3003";
        expected.location = 187;
        expected.lat = 24.074;
        expected.lon = 120.340;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:15");
        expected.p1 = 13.00;
        expected.p2 = 12.00;

        Assertions.assertEquals(expected,
                parser.readRecord("397;PMS3003;187;24.074;120.340;2019-04-01T00:00:15;13.00;12.00;\n"));
        expected.p0 = 1.0;
        UnifiedSensorReading parsed = parser.readRecord("397;PMS3003;187;24.074;120.340;2019-04-01T00:00:15;13.00;12.00;1.0\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.PMS3003)));
    }

    @Test
    void testReadRecordPMS5003() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.PMS5003);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 20157;
        expected.sensorType = "PMS5003";
        expected.location = 10239;
        expected.lat = 56.820;
        expected.lon = 24.415;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:25");
        expected.p1 = 0.00;
        expected.p2 = 0.00;
        expected.p0 = 0.00;
        UnifiedSensorReading parsed = parser.readRecord("20157;PMS5003;10239;56.820;24.415;2019-04-01T00:00:25;0.00;0.00;0.00\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.PMS5003)));
    }

    @Test
    void testReadRecordPMS7003() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.PMS7003);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 13030;
        expected.sensorType = "PMS7003";
        expected.location = 6582;
        expected.lat = 54.644;
        expected.lon = 9.760;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:03");
        expected.p1 = 1.3;
        expected.p2 = 1.3;

        Assertions.assertEquals(expected,
                parser.readRecord("13030;PMS7003;6582;54.644;9.760;2019-04-01T00:00:03;1.3;1.3;\n"));
        expected.p0 = 1.0;
        UnifiedSensorReading parsed = parser.readRecord("13030;PMS7003;6582;54.644;9.760;2019-04-01T00:00:03;1.3;1.3;1.0\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.PMS7003)));
    }

    @Test
    void testReadRecordPPD42NS() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.PPD42NS);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 107;
        expected.sensorType = "PPD42NS";
        expected.location = 49;
        expected.lat = 48.531;
        expected.lon = 9.200;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:06");
        expected.p1 = 961.41;
        expected.durP1 = 557804.;
        expected.ratioP1 = 1.86;
        expected.p2 = 67.06;
        expected.durP2 = 38364.;
        expected.ratioP2 = 0.13;
        UnifiedSensorReading parsed = parser.readRecord("107;PPD42NS;49;48.531;9.200;2019-04-01T00:00:06;961.41;557804;1.86;67.06;38364;0.13\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.PPD42NS)));
    }

    @Test
    void testReadRecordSDS011() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.SDS011);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 6787;
        expected.sensorType = "SDS011";
        expected.location = 3431;
        expected.lat = 47.966;
        expected.lon = 10.267;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:00");
        expected.p1 = 12.93;
        expected.p2 = 9.85;

        Assertions.assertEquals(expected,
                parser.readRecord("6787;SDS011;3431;47.966;10.267;2019-04-01T00:00:00;12.93;;;9.85;;\n"));
        expected.durP1 = 1.0;
        expected.ratioP1 = 2.1;
        expected.durP2 = 3.2;
        expected.ratioP2 = 4.3;
        UnifiedSensorReading parsed = parser.readRecord("6787;SDS011;3431;47.966;10.267;2019-04-01T00:00:00;12.93;1.0;2.1;9.85;3.2;4.3\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.SDS011)));
    }

    @Test
    void testReadRecordUnified() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        SensorReadingParser parser = new SensorReadingParser(Type.UNIFIED);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 15845;
        expected.sensorType = "UNIFIED";
        expected.location = 8022;
        expected.lat = 52.498;
        expected.lon = 13.465;
        expected.timestamp = Timestamp.valueOf("2019-04-01 00:00:00");
        expected.pressure = 102401.00;
        expected.altitude = 1.0;
        expected.pressure_sealevel = 2.1;
        expected.temperature = 10.40;
        expected.humidity = 3.2;
        expected.p1 = 4.3;
        expected.p2 = 5.4;
        expected.p0 = 6.5;
        expected.durP1 = 7.6;
        expected.ratioP1 = 8.7;
        expected.durP2 = 9.8;
        expected.ratioP2 = 0.9;
        UnifiedSensorReading parsed = parser.readRecord("15845;UNIFIED;8022;52.498;13.465;2019-04-01T00:00:00;" +
                "102401.00;1.0;2.1;10.40;3.2;4.3;5.4;6.5;7.6;8.7;9.8;0.9\n");
        Assertions.assertEquals(expected, parsed);
        Assertions.assertEquals(parsed, parser.readRecord(parsed.toString(Type.UNIFIED)));
    }
}
