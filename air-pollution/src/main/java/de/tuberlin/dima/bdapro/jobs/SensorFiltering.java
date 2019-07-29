package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.sensor.SensorReadingFormatter;
import de.tuberlin.dima.bdapro.sensor.Type;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

import java.util.ArrayList;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Flink workflow that filters the sensor data to only include data of sensors whose closest weather station is
 * contained in the downloaded weather data.
 * <p>
 * This workflow contains methods to calculate the filtered dataset from scratch and return it directly, as well as to
 * precalculate the filtered data and store it to file, from where it can then be loaded again.
 *
 * @author Hendrik Makait
 */
public class SensorFiltering extends UnifiedSensorJob {
    // relative path of the raw sensor data
    private static String rawSensorDataPath = "raw/csv_per_month";
    // relative path to store filtered data in
    private static String filteredSensorDataPath = "intermediate/filtered";
    // relative path to retrieve the file of mapped locations
    private static String filterBasePath = "intermediate/";

    /**
     * Calculates the filtered dataset for all sensor data in "data_directory" and stores it in individual
     * csv files per type.
     */
    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");
        cacheFilteredSensorData(dataDirectory, env);

        env.execute("Filter Dataset");
    }

    /**
     * Returns a filtered dataset of all sensor readings found in the specified data directory. If cached, it loads
     * precalculated data from file, otherwise it loads the raw data and filters it.
     *
     * @param cached        Whether to load precalculated data from file or load raw data and filter it.
     * @param dataDirectory The base path of the data directory.
     * @param env           An execution environment to use.
     * @return the dataset of filtered sensor.
     */
    public static DataSet<UnifiedSensorReading> getFilteredSensors(boolean cached, String dataDirectory, ExecutionEnvironment env) {
        if (cached) {
            Path sensorDataBasePath = new Path(dataDirectory, filteredSensorDataPath);
            return readAllSensors(sensorDataBasePath.getPath(), env, false);
        } else {
            ArrayList<DataSet<UnifiedSensorReading>> filteredSensorList = new ArrayList<>();
            DataSet<Integer> acceptedSensors = readAcceptedSensors(env, dataDirectory);
            for (Type sensorType : Type.values()) {
                filteredSensorList.add(filterSensor(sensorType, dataDirectory, acceptedSensors, env));
            }
            return unionAll(filteredSensorList);
        }
    }

    /**
     * Returns a filtered dataset of sensor readings of a given type found in the specified data directory. If cached,
     * it loads precalculated data from file, otherwise it loads the raw data and filters it.
     *
     * @param sensorType    The type of sensor to filter
     * @param cached        Whether to load precalculated data from file or load raw data and filter it.
     * @param dataDirectory The base path of the data directory.
     * @param env           An execution environment to use.
     * @return the dataset of filtered sensor.
     */
    public static DataSet<UnifiedSensorReading> getFilteredSensor(Type sensorType, boolean cached, String dataDirectory, ExecutionEnvironment env) {
        if (cached) {
            Path sensorDataBasePath = new Path(dataDirectory, filteredSensorDataPath);
            return readSensor(sensorType, sensorDataBasePath.getPath(), env, false);
        } else {
            DataSet<Integer> acceptedSensors = readAcceptedSensors(env, dataDirectory);
            return filterSensor(sensorType, dataDirectory, acceptedSensors, env);
        }
    }

    /**
     * Precalculates the filtered dataset for all sensor data found in specified data directory and stores it to file.
     *
     * @param dataDirectory The base path of the given data directory.
     * @param env           An execution environment to use.
     */
    private static void cacheFilteredSensorData(String dataDirectory, ExecutionEnvironment env) {
        DataSet<Integer> acceptedSensors = readAcceptedSensors(env, dataDirectory);
        for (Type sensorType : Type.values()) {
            DataSet<UnifiedSensorReading> filteredData = filterSensor(sensorType, dataDirectory, acceptedSensors, env);
            Path outputFilePath = new Path(dataDirectory, String.format("intermediate/filtered/%s.csv", getSensorPattern(sensorType)));
            filteredData.writeAsFormattedText(outputFilePath.toString(), OVERWRITE, new SensorReadingFormatter(sensorType)).setParallelism(1);
        }
    }

    /**
     * Loads all sensor data of a given type and filters it such that the result only includes readings from the
     * specified accepted sensor locations.
     *
     * @param sensorType      The type of the sensor data to load from disk
     * @param dataDirectory   The base path of the data directory.
     * @param acceptedSensors The given sensor locations to include in the result.
     * @param env             An execution environment to use.
     * @return the dataset of sensor of filtered sensor readings
     */
    private static DataSet<UnifiedSensorReading> filterSensor(Type sensorType, String dataDirectory, DataSet<Integer> acceptedSensors, ExecutionEnvironment env) {
        Path sensorDataBasePath = new Path(dataDirectory, rawSensorDataPath);
        DataSet<UnifiedSensorReading> sensorData = readSensor(sensorType, sensorDataBasePath.toString(), env);
        return filterSensorData(sensorData, acceptedSensors);
    }

    /**
     * Filters the given sensor readings such that the result only includes readings from the specified accepted sensor
     * locations.
     *
     * @param sensorData      The given sensor dataset.
     * @param acceptedSensors The given sensor locations to include in the result.
     * @return the dataset of sensor of filtered sensor readings
     */
    private static DataSet<UnifiedSensorReading> filterSensorData(DataSet<UnifiedSensorReading> sensorData, DataSet<Integer> acceptedSensors) {
        DataSet<Tuple2<UnifiedSensorReading, Integer>> joinResult = sensorData.joinWithTiny(acceptedSensors).where((UnifiedSensorReading reading) -> reading.sensorId).equalTo((Integer sensorId) -> sensorId);
        return joinResult.map((Tuple2<UnifiedSensorReading, Integer> tuple) -> tuple.f0);
    }

    /**
     * Reads the sensor to station mapping and returns a distinct dataset of contained sensor locations.
     *
     * @param env      An execution environment to use.
     * @param basePath The base path of the data directory.
     * @return the dataset of contained sensor locations
     */
    private static DataSet<Integer> readAcceptedSensors(ExecutionEnvironment env, String basePath) {
        DataSet<Tuple1<Double>> acceptedSensorData = env.readCsvFile(new Path(new Path(basePath, filterBasePath), "berlin_enrichable_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields(0x1).types(Double.class);
        return acceptedSensorData.map((Tuple1<Double> tuple) -> (int) Math.round(tuple.f0));
    }
}
