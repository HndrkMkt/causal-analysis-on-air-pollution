package de.tuberlin.dima.bdapro.dataIntegration.sensor;

import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileStatus;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Test cases for the {@link SensorReadingCsvInputFormat}
 *
 * @author Hendrik Makait
 */
class SensorReadingCsvInputFormatTest {
    @Test
    void testSkipFirstLine() throws IOException {
        SensorReadingCsvInputFormat format = new SensorReadingCsvInputFormat(
                new Path("file:///some/file/that/will/not/be/read"), Type.UNIFIED);

        final String myString = "my mocked line 1\nmy mocked line 2\n";
        final FileInputSplit split = createTempFile(myString);
        format.open(split);
        Assertions.assertEquals(new Long(17), format.getCurrentState());
    }

    @Test
    void testReadRecord() throws IOException {
        SensorReadingCsvInputFormat format = new SensorReadingCsvInputFormat(
                new Path("file:///some/file/that/will/not/be/read"), Type.BME280);

        final String myString = "sensor_id;sensor_type;location;lat;lon;timestamp;pressure;altitude;pressure_sealevel;temperature;humidity\n" +
                "1;BME280;2;3.4;5.6;2019-01-01T12:00:00;7.8;9.0;1.2;3.4;5.6\n" +
                "2;BME280;2;3.4;5.6;2019-01-01T12:00:00;7.8;9.0;1.2;3.4\n";
        final FileInputSplit split = createTempFile(myString);
        format.setBufferSize(100);
        format.open(split);
        UnifiedSensorReading expected = new UnifiedSensorReading();
        expected.sensorId = 1;
        expected.sensorType = "BME280";
        expected.location = 2;
        expected.lat = 3.4;
        expected.lon = 5.6;
        expected.timestamp = Timestamp.valueOf("2019-01-01 12:00:00");
        expected.pressure = 7.8;
        expected.altitude = 9.0;
        expected.pressure_sealevel = 1.2;
        expected.temperature = 3.4;
        expected.humidity = 5.6;
        UnifiedSensorReading first = format.nextRecord(new UnifiedSensorReading());
        Assertions.assertEquals(expected, first);
    }

    @Test
    void testAcceptFileWithNesting(@TempDir java.nio.file.Path tempDir) {
        SensorReadingCsvInputFormat fileFormat = new SensorReadingCsvInputFormat(new Path(tempDir.toString()), Type.UNIFIED);
        fileFormat.setNestedFileEnumeration(true);
        fileFormat.setFilesFilter(new GlobFilePathFilter(Arrays.asList("**/", "**/foo.csv"), new ArrayList<>()));
        Assertions.assertTrue(fileFormat.acceptFile(new LocalFileStatus(tempDir.resolve("foo.csv").toFile(), new LocalFileSystem())));
        Assertions.assertTrue(fileFormat.acceptFile(new LocalFileStatus(tempDir.resolve("./bar/foo.csv").toFile(), new LocalFileSystem())));
        Assertions.assertTrue(fileFormat.acceptFile(new LocalFileStatus(tempDir.resolve("./bar/foo.csv").toFile(), new LocalFileSystem())));
        Assertions.assertTrue(fileFormat.acceptFile(new LocalFileStatus(tempDir.resolve("./bar/baz/foo.csv").toFile(), new LocalFileSystem())));
        Assertions.assertFalse(fileFormat.acceptFile(new LocalFileStatus(tempDir.resolve("foo.csv2").toFile(), new LocalFileSystem())));
        // Do not accept magic/hidden directories
        Assertions.assertFalse(fileFormat.acceptFile(new LocalFileStatus(tempDir.resolve("./.bar/").toFile(), new LocalFileSystem())));
        Assertions.assertFalse(fileFormat.acceptFile(new LocalFileStatus(tempDir.resolve("./_bar/").toFile(), new LocalFileSystem())));
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
