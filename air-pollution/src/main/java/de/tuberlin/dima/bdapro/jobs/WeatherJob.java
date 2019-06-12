package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.parsers.WeatherReadingParser;
import de.tuberlin.dima.bdapro.weather.Field;
import de.tuberlin.dima.bdapro.weather.NullableCsvInputFormat;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;

import java.util.List;

public class WeatherJob {

    private static String weatherDataPath = "../data/raw/weather/weather_data.csv";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<WeatherReading> weatherReadingDataSet = readWeather(env, weatherDataPath, WeatherReading.class, WeatherReading.getFields());

        env.execute("Flink Batch Java API Skeleton");
    }

    private static <T extends WeatherReading> DataSet<T> readWeather(ExecutionEnvironment env, String weatherDataPath, Class<T> clazz, List<Field> fields) {
        NullableCsvInputFormat<T> fileFormat = new NullableCsvInputFormat<>(new Path(weatherDataPath),
                new WeatherReadingParser<>(clazz, fields));
        return env.createInput(fileFormat, TypeInformation.of(clazz)).setParallelism(1);
    }
}
