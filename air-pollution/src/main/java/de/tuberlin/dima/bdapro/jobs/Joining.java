package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.featureTable.Column;
import de.tuberlin.dima.bdapro.featureTable.FeatureTable;
import de.tuberlin.dima.bdapro.featureTable.IColumn;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;


public class Joining extends UnifiedSensorJob {

    private static String weatherDataPath = "raw/weather/weather_data.csv";
    private static String filterBasePath = "intermediate/";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");
        FeatureTable sensor = Aggregation.generateFeatureTable(env, dataDirectory, 60, tEnv);
        FeatureTable sensorStationMapping = generateSensorStationMappingFeatureTable(dataDirectory, env, tEnv);
        FeatureTable weather = WeatherJob.generateFeatureTable(env, tEnv);
        FeatureTable mappedSensors = sensor.join(sensorStationMapping, sensor.getKeyColumns(), "sensor_station_mapping_location = sensor_location", tEnv);
        FeatureTable result = mappedSensors.join(weather, sensor.getKeyColumns(),
                "sensor_station_mapping_closest_weather_station = weather_location AND " +
                "FLOOR(sensor_timestamp TO HOUR) = FLOOR(weather_time TO HOUR)", tEnv);
        Path outputPath = new Path(dataDirectory, "processed/causalDiscoveryData.csv");
        result.write(outputPath, tEnv);
        env.execute("Joined Dataset");
    }

    public static FeatureTable generateSensorStationMappingFeatureTable(String dataDirectory, ExecutionEnvironment env, BatchTableEnvironment tEnv) {
        DataSet<Tuple2<Double, String>> acceptedSensorData = env.readCsvFile(new Path(new Path(dataDirectory, filterBasePath), "berlin_enrichable_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields("00001100").types(Double.class, String.class);
        Table table = tEnv.fromDataSet(acceptedSensorData,"location," + "closest_weather_station").distinct();
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("location", TypeInformation.of(Double.class), false));
        columns.add(new Column("closest_weather_station", TypeInformation.of(String.class), false));
        return new FeatureTable("sensor_station_mapping", table, columns, columns, tEnv);
    }
}
