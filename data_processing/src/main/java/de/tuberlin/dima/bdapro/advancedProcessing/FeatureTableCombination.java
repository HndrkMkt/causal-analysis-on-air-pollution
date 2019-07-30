package de.tuberlin.dima.bdapro.advancedProcessing;

import de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows.SensorFeatureTableGenerator;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows.UnifiedSensorWorkflow;
import de.tuberlin.dima.bdapro.dataIntegration.weather.WeatherWorkflow;
import de.tuberlin.dima.bdapro.featureTable.Column;
import de.tuberlin.dima.bdapro.featureTable.BasicColumn;
import de.tuberlin.dima.bdapro.featureTable.FeatureTable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink workflow that combines the feature tables generated from sensor and weather data using a mapping feature table.
 *
 * @author Hendrik Makait
 */
public class FeatureTableCombination extends UnifiedSensorWorkflow {
    // relative path of the downloaded weather data
    private static String weatherDataPath = "raw/weather/weather_data.csv";
    // relative path of the directory containing the precalculated filtered sensor data
    private static String filterBasePath = "intermediate/";

    /**
     * Generates the combined feature table and stores it to disk.
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");

        FeatureTable result = generateCombinedFeatureTable(env, tEnv, dataDirectory);

        Path outputPath = new Path(dataDirectory, "processed/causalDiscoveryData.csv");
        result.write(outputPath, tEnv);
        env.execute("Generate Combined Feature Table");
    }

    /**
     * Generates a combined feature table by combining the sensor and weather feature tables.
     *
     * @param env           An execution environment.
     * @param tEnv          A table environment.
     * @param dataDirectory The base path of the data directory.
     * @return the combined feature table.
     */
    private static FeatureTable generateCombinedFeatureTable(ExecutionEnvironment env, BatchTableEnvironment tEnv, String dataDirectory) {
        FeatureTable sensor = SensorFeatureTableGenerator.generateFeatureTable(env, dataDirectory, 60, tEnv);
        FeatureTable sensorStationMapping = generateSensorStationMappingFeatureTable(dataDirectory, env, tEnv);
        FeatureTable weather = WeatherWorkflow.generateFeatureTable(env, tEnv);
        FeatureTable mappedSensors = sensor.join(sensorStationMapping, sensor.getKeyColumns(), "sensor_station_mapping_location = sensor_location", tEnv);
        return mappedSensors.join(weather, sensor.getKeyColumns(),
                "sensor_station_mapping_closest_weather_station = weather_location AND " +
                        "FLOOR(sensor_timestamp TO HOUR) = FLOOR(weather_time TO HOUR)", tEnv);
    }

    /**
     * Creates the feature table mapping sensor locations to weather stations by loading the precalculated mapping
     * data from disk.
     *
     * @param dataDirectory The base path of the data directory.
     * @param env           An execution environment to use.
     * @param tEnv          A table environment to use.
     * @return the sensor station mapping feature table
     */
    public static FeatureTable generateSensorStationMappingFeatureTable(String dataDirectory, ExecutionEnvironment env, BatchTableEnvironment tEnv) {
        DataSet<Tuple2<Double, String>> acceptedSensorData = env.readCsvFile(new Path(new Path(dataDirectory, filterBasePath), "berlin_enrichable_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields("00001100").types(Double.class, String.class);
        Table table = tEnv.fromDataSet(acceptedSensorData, "location," + "closest_weather_station").distinct();
        List<Column> columns = new ArrayList<>();
        columns.add(new BasicColumn("location", TypeInformation.of(Double.class), false));
        columns.add(new BasicColumn("closest_weather_station", TypeInformation.of(String.class), false));
        return new FeatureTable("sensor_station_mapping", table, columns, columns, tEnv);
    }
}
