package de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows;

import de.tuberlin.dima.bdapro.dataIntegration.sensor.UnifiedSensorReading;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the {@link SensorStatistics}, also tests {@link UnifiedSensorWorkflow} functionality.
 *
 * @author Hendrik Makait
 */
public class SensorStatisticsTest {
    @Test
    void testReadUncompressedFiles() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<UnifiedSensorReading> dataset = SensorStatistics.readAllSensors("src/test/resources/data", env, false);
        Assertions.assertEquals(6, dataset.count());
    }

    @Test
    void testReadCompressedFiles() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<UnifiedSensorReading> dataset = SensorStatistics.readAllSensors("src/test/resources/data", env, true);
        Assertions.assertEquals(6, dataset.count());
    }
}
