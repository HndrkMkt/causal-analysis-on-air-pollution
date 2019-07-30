package de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows;

import de.tuberlin.dima.bdapro.advancedProcessing.featureTable.FeatureTable;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.UnifiedSensorReading;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the {@link SensorFiltering}
 *
 * @author Hendrik Makait
 */
public class SensorFeatureTableGenerationTest {
    @Test
    void testFeatureTableGeneration() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        FeatureTable dataset = SensorFeatureTableGeneration.generateFeatureTable(false, env, "src/test/resources/data", 60, tEnv);
        Assertions.assertEquals(6, tEnv.toDataSet(dataset.getData(), Row.class).count());
    }
}
