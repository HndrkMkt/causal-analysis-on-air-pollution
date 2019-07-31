package de.tuberlin.dima.bdapro.dataIntegration.weather.Workflow;

import de.tuberlin.dima.bdapro.advancedProcessing.featureTable.FeatureTable;
import de.tuberlin.dima.bdapro.dataIntegration.weather.WeatherWorkflow;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the {@link WeatherWorkflow}
 *
 * @author Ricardo Salazar
 */

public class WeatherWorkFlowTest {
    @Test
    void testFiltering() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        FeatureTable dataset = WeatherWorkflow.generateFeatureTable("src/test/resources/data",env,tEnv);
        Assertions.assertEquals(2, tEnv.toDataSet(dataset.getData(), Row.class).count());
    }
}
