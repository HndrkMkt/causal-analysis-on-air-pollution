package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.featureTable.Column;
import de.tuberlin.dima.bdapro.featureTable.FeatureTable;
import de.tuberlin.dima.bdapro.parsers.WeatherReadingParser;
import de.tuberlin.dima.bdapro.weather.Field;
import de.tuberlin.dima.bdapro.weather.WeatherReadingInputFormat;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

public class WeatherJob {
    private static String weatherDataPath = "data/raw/weather/weather_data.csv";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<WeatherReading> weatherReadingDataSet = readWeather(env, weatherDataPath, WeatherReading.getFields());
        List<WeatherReading> result = weatherReadingDataSet.collect();
        env.execute("Flink Batch Java API Skeleton");
    }

    public static FeatureTable generateFeatureTable(ExecutionEnvironment env, BatchTableEnvironment batchTableEnvironment) {
        DataSet<WeatherReading> weatherReadingDataSet = readWeather(env, weatherDataPath, WeatherReading.getFields());
        Table weatherTable = batchTableEnvironment.fromDataSet(weatherReadingDataSet);
        List<? extends Column> columns = WeatherReading.getFields();
        List<Column> keyColumns = new ArrayList<>();
        String[] keyColumnNames = {"location", "time"};
        for (String keyColumnName : keyColumnNames) {
            for (Column column : columns) {
                if (column.getName().equals(keyColumnName)) {
                    keyColumns.add(column);
                }
            }
        }
        FeatureTable featureTable = new FeatureTable("weather", weatherTable, columns, keyColumns, batchTableEnvironment);
        return featureTable;
    }

    public static DataSet<WeatherReading> readWeather(ExecutionEnvironment env, String weatherDataPath, List<Field> fields) {
        WeatherReadingInputFormat fileFormat = new WeatherReadingInputFormat(new Path(weatherDataPath),
                new WeatherReadingParser(fields));
        return env.createInput(fileFormat, TypeInformation.of(WeatherReading.class)).setParallelism(1);
    }
}
