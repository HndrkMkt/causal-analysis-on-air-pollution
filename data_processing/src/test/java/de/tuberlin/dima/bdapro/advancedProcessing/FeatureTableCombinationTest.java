package de.tuberlin.dima.bdapro.advancedProcessing;

import de.tuberlin.dima.bdapro.advancedProcessing.featureTable.FeatureTable;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows.SensorFeatureTableGeneration;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows.SensorFiltering;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the {@link FeatureTableCombination}
 *
 * @author Hendrik Makait
 */
public class FeatureTableCombinationTest {
    @Test
    void testFeatureTableCombination() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        FeatureTable dataset = FeatureTableCombination.generateCombinedFeatureTable(false, env, tEnv, "src/test/resources/data");
        Assertions.assertEquals(3, tEnv.toDataSet(dataset.getData(), Row.class).count());
    }
}
