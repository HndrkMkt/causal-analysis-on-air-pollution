package de.tuberlin.dima.bdapro.dataIntegration.weather;

import de.tuberlin.dima.bdapro.advancedProcessing.featureTable.Column;
import de.tuberlin.dima.bdapro.advancedProcessing.featureTable.FeatureTable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * This class materialize the weather input data from OpenWeatherMap web APIs and creates a new {@link FeatureTable} containing
 * the requested fields previously defined in the logic of the WeatherReading class.
 */
public class WeatherWorkflow {
    private static final String weatherDataPath = "data/raw/weather/weather_data.csv";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<WeatherReading> weatherReadingDataSet = readWeather(env, weatherDataPath, WeatherReading.getFields());
        List<WeatherReading> result = weatherReadingDataSet.collect();
        env.execute("Flink Batch Java API Skeleton");
    }

    /**
     * Creates a new {@link FeatureTable} from the weather input data
     * @param env the Flink execution environment
     * @param batchTableEnvironment the Flink batch table environment
     * @return a {@link FeatureTable} containing the requested fields previously defined in the logic of the WeatherReading class.
     */
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

    /**
     * Creates a new {@link WeatherReading} DataSet from the csv input weather data from OpenWeatherMap web APIs and the requested weather
     * fields defined in the logic of the WeatherReading class.
     *
     * @param env the Flink execution environment
     * @param weatherDataPath a valid {@link Path} containing the csv input weather data from OpenWeatherMap web APIs
     * @param fields a {@link List} containing the requested fields defined in the logic of the WeatherReading class.
     * @return a {@link DataSet} with the requested weather fields defined in the logic of the WeatherReading class.
     */
    private static DataSet<WeatherReading> readWeather(ExecutionEnvironment env, String weatherDataPath, List<Field> fields) {
        WeatherReadingInputFormat fileFormat = new WeatherReadingInputFormat(new Path(weatherDataPath),
                new WeatherReadingParser(fields));
        return env.createInput(fileFormat, TypeInformation.of(WeatherReading.class)).setParallelism(1);
    }
}
