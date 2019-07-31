package de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows;

import de.tuberlin.dima.bdapro.dataIntegration.sensor.UnifiedSensorReading;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the {@link SensorFiltering}
 *
 * @author Hendrik Makait
 */
public class SensorFilteringTest {
    @Test
    void testFiltering() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<UnifiedSensorReading> dataset = SensorFiltering.getFilteredSensors(false, "src/test/resources/data", env);
        Assertions.assertEquals(11, dataset.count());
    }
}
