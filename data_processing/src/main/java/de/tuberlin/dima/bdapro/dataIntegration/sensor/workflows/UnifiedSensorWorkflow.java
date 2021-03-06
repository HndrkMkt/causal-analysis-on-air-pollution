package de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows;

import de.tuberlin.dima.bdapro.dataIntegration.sensor.SensorReadingCsvInputFormat;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.Type;
import de.tuberlin.dima.bdapro.dataIntegration.sensor.UnifiedSensorReading;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;

import java.util.*;

/**
 * Base class for Flink workflows loading sensor data from disk.
 *
 * @author Hendrik Makait
 */
abstract public class UnifiedSensorWorkflow {
    protected static final Map<Type, String> sensorPatterns = new HashMap<Type, String>() {{
        put(Type.BME280, "bme280");
        put(Type.BMP180, "bmp180");
        put(Type.DHT22, "dht22");
        put(Type.DS18B20, "ds18b20");
        put(Type.HPM, "hpm");
        put(Type.HTU21D, "htu21d");
        put(Type.PMS3003, "pms3003");
        put(Type.PMS5003, "pms5003");
        put(Type.PMS7003, "pms7003");
        put(Type.PPD42NS, "ppd42ns");
        put(Type.SDS011, "sds011");
        put(Type.UNIFIED, "unified");
    }};

    /**
     * Returns a string pattern that identifies files containing data of a given sensor type.
     *
     * @param sensorType The type of the sensor.
     * @return the string pattern identifying files for the given type
     */
    protected static String getSensorPattern(Type sensorType) {
        return sensorPatterns.get(sensorType);
    }

    /**
     * Recursively retrieves all files containing data of a given sensor type from the base directory and loads the data
     * into a dataset of {@link UnifiedSensorReading}.
     *
     * @param sensorType         The sensor type to load.
     * @param sensorDataBasePath The base path containing all data.
     * @param env                A execution environment to use
     * @param compressed         Whether the files are compressed as csv.gz or raw csv.
     * @return the dataset of all data for the given sensor type
     */
    protected static DataSet<UnifiedSensorReading> readSensor(Type sensorType, String sensorDataBasePath, ExecutionEnvironment env, boolean compressed) {
        String suffix = compressed ? ".csv.gz" : ".csv";
        String sensorPattern = String.format("**/*%s" + suffix, getSensorPattern(sensorType));
        SensorReadingCsvInputFormat fileFormat = new SensorReadingCsvInputFormat(new Path(sensorDataBasePath), sensorType);
        fileFormat.setNestedFileEnumeration(true);
        fileFormat.setFilesFilter(new GlobFilePathFilter(Arrays.asList("**/", sensorPattern), new ArrayList<>()));
        return env.createInput(fileFormat, TypeInformation.of(UnifiedSensorReading.class)).setParallelism(1)
                .filter((UnifiedSensorReading reading) -> reading.sensorId != null);
    }

    /**
     * Recursively retrieves all csv.gz files containing data of a given sensor type from the base directory and loads the data
     * into a dataset of {@link UnifiedSensorReading} (Wrapper around readSensor(...,compressed=True) for backwards compatibility.
     *
     * @param sensorType         The sensor type to load.
     * @param sensorDataBasePath The base path containing all data.
     * @param env                A execution environment to use
     * @return the dataset of all data for the given sensor type
     */
    protected static DataSet<UnifiedSensorReading> readSensor(Type sensorType, String sensorDataBasePath, ExecutionEnvironment env) {
        return readSensor(sensorType, sensorDataBasePath, env, true);
    }

    /**
     * Recursively retrieves all files containing sensor data of all types from the base directory and loads the data into a
     * dataset of {@link UnifiedSensorReading}.
     *
     * @param sensorDataBasePath The base path containing all data.
     * @param env                A execution environment to use
     * @param compressed         Whether the files are compressed as csv.gz or raw csv.
     * @return the dataset of all sensor data
     */
    protected static DataSet<UnifiedSensorReading> readAllSensors(String sensorDataBasePath, ExecutionEnvironment env, boolean compressed) {
        ArrayList<DataSet<UnifiedSensorReading>> sensorReadingsList = new ArrayList<>();
        for (Type sensorType : Type.values()) {
            sensorReadingsList.add(readSensor(sensorType, sensorDataBasePath, env, compressed));
        }
        return unionAll(sensorReadingsList);
    }

    /**
     * Helper function that performs a union operation all datasets in the given list and returns their combined dataset.
     *
     * @param list A list of datasets to unionize.
     * @param <T>  The type of the individual entries in the datasets.
     * @return the unionized dataset
     */
    protected static <T> DataSet<T> unionAll(List<DataSet<T>> list) {
        DataSet<T> unionDataset = null;
        for (DataSet<T> dataset : list) {
            if (unionDataset == null) {
                unionDataset = dataset;
            } else {
                unionDataset = unionDataset.union(dataset);
            }
        }
        return unionDataset;
    }
}
