package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.sensors.Type;
import de.tuberlin.dima.bdapro.sensors.UnifiedSensorReading;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
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

        DataSet<Tuple4<Double, Double, Double, String>> acceptedSensorData = env.readCsvFile(new Path(new Path(dataDirectory, filterBasePath), "berlin_enrichable_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields("00111100").types(Double.class, Double.class, Double.class, String.class);
        Path weatherDataBasePath = new Path(dataDirectory, weatherDataPath);

        DataSet<WeatherReading> weatherReadingDataSet = WeatherJob.readWeather(env, weatherDataBasePath.getPath(), WeatherReading.class, WeatherReading.getFields());

        DataSet<UnifiedSensorReading> filteredSensorData = Filtering.getFilteredSensors(true, dataDirectory, env);
        tEnv.registerTable("unified_sensor_data", Aggregation.aggregateSensorData(filteredSensorData, 60, env, tEnv));
        tEnv.registerDataSet("weather", weatherReadingDataSet);
        tEnv.registerDataSet("accepted_sensors", acceptedSensorData, "longitude," +
                "latitude," +
                "location," +
                "closest_weather_station"
        );

        Table distinctLocationMappings = tEnv.scan("accepted_sensors").distinct();
        Table sensorsWithWeatherStationsNames = tEnv.sqlQuery(
                "SELECT s.*, a.closest_weather_station FROM unified_sensor_data s JOIN " + distinctLocationMappings + " a ON s.location=a.location");

        StringBuilder sqlBuilder = new StringBuilder();
        List<String> aggregatedFielNames = new ArrayList<>();
        for (String fieldName : Aggregation.getAggregationFieldNames()) {
            aggregatedFielNames.add(String.format("a.`%s`", fieldName));
        }
        String aggregatedFields = StringUtils.join(aggregatedFielNames, ", ");
        sqlBuilder.append("SELECT " + aggregatedFields);
        for (String fieldName : WeatherReading.getMeasurementFieldNames()) {
            sqlBuilder.append(String.format(", w.`%s`", fieldName));
        }
        sqlBuilder.append(" FROM " + sensorsWithWeatherStationsNames + " a ")
                .append("JOIN weather w ")
                .append("ON a.closest_weather_station = w.location AND FLOOR(a.`timestamp` TO HOUR) = FLOOR(w.`time` TO HOUR)");

        String spatio_temporal_join = sqlBuilder.toString();

        Table result = tEnv.sqlQuery(spatio_temporal_join);
        Path outputPath = new Path(dataDirectory, "processed/causalDiscoveryData.csv");
        TableSink<Row> sink = new CsvTableSink(outputPath.getPath(), ";", 1, OVERWRITE);

        String[] fieldNames = Aggregation.getAggregationFieldNames();
        fieldNames = ArrayUtils.addAll(fieldNames, WeatherReading.getMeasurementFieldNames());

        TypeInformation[] fieldTypes = Aggregation.getAggregationFieldTypes();
        fieldTypes = ArrayUtils.addAll(fieldTypes, WeatherReading.getMeasurementFieldTypes());
        tEnv.registerTableSink("output", fieldNames, fieldTypes, sink);
        result.insertInto("output");

//        DataSet<JoinWeather> joinWeatherResult = tEnv.toDataSet(result, JoinWeather.class);
//        List<JoinWeather> foo = joinWeatherResult.collect();
        env.execute("Joined Dataset");

    }
//
//    public static class JoinStations {
//        // Common Fields
//        public Integer sensorId;
//        public String sensorType;
//        public Integer location;
//        public Double lat;
//        public Double lon;
//        public Timestamp timestamp;
//
//        // Sensor specific fields
//        public Double pressure;
//        public Double altitude;
//        public Double pressure_sealevel;
//        public Double temperature;
//        public Double humidity;
//
//        public Double p1;
//        public Double p2;
//        public Double p0;
//        public Double durP1;
//        public Double ratioP1;
//        public Double durP2;
//        public Double ratioP2;
//
//        // weather station
//        public String closest_weather_station;
//
//        public JoinStations(Integer sensorId, String sensorType, Integer location, Double lat, Double lon, Timestamp timestamp,
//                            Double pressure, Double altitude, Double pressure_sealevel, Double temperature, Double humidity, Double p1,
//                            Double p2, Double p0, Double durP1, Double ratioP1, Double durP2, Double ratioP2, String closest_weather_station) {
//            super();
//            this.sensorId = sensorId;
//            this.sensorType = sensorType;
//            this.location = location;
//            this.lat = lat;
//            this.lon = lon;
//            this.timestamp = timestamp;
//            this.pressure = pressure;
//            this.altitude = altitude;
//            this.pressure_sealevel = pressure_sealevel;
//            this.temperature = temperature;
//            this.humidity = humidity;
//            this.p1 = p1;
//            this.p2 = p2;
//            this.p0 = p0;
//            this.durP1 = durP1;
//            this.ratioP1 = ratioP1;
//            this.durP2 = durP2;
//            this.ratioP2 = ratioP2;
//            this.closest_weather_station = closest_weather_station;
//        }
//
//        public JoinStations() {
//            super();
//        }
//
//        //@Override
//        //public String toString() {
//        //    return "StationJoin [sensorID=" + sensorId + ", sensorType=" + sensorType + ", location=" + location +
//        //            ", lat=" + lat + ", lon=" + lon + ", timestamp=" + timestamp + ", pressure="+ pressure+
//        //            ", altitude=" + altitude + ", pressure_sealevel=" + pressure_sealevel + ",temperature=" + temperature+
//        //            ", humidity="+ humidity+ ", p1="+ p1 + ", p2=" + p2 + ", p0=" + p0 +
//        //            ", durP1=" + durP1 +
//        //            ", ratioP1=" + ratioP1 + ", durP2=" +durP2 + ", ratioP2=" + ratioP2 +", closest_weather_station=" + closest_weather_station +
//        //            "]";
//        //}
//
//    }

    public static class JoinWeather {
        // Common Fields
//        public Integer sensorId;
//        public String sensorType;
        public Integer location;
        public Double lat;
        public Double lon;
        public Timestamp timestamp;

        // Sensor specific fields
        public Double pressure;
        public Double altitude;
        public Double pressure_sealevel;
        public Double temperature;
        public Double humidity;

        public Double p1;
        public Double p2;
        public Double p0;
        public Double durP1;
        public Double ratioP1;
        public Double durP2;
        public Double ratioP2;

        // weather station
        public String closest_weather_station;
        public Double apparent_temperature;
        public Double cloud_cover;
        public Double dew_point;
        public Double ozone;
        public Double precip_intensity;
        public Double precip_probability;
        public String precip_type;
        public Double uv_index;
        public Double visibility;
        public Double wind_bearing;
        public Double wind_gust;
        public Double wind_speed;

        public JoinWeather(//Integer sensorId, String sensorType,
                           Integer location, Double lat, Double lon, Timestamp timestamp,
                           Double pressure, Double altitude, Double pressure_sealevel, Double temperature, Double humidity, Double p1,
                           Double p2, Double p0, Double durP1, Double ratioP1, Double durP2, Double ratioP2, String closest_weather_station,
                           Double apparent_temperature, Double cloud_cover, Double dew_point, Double ozone, Double precip_intensity,
                           Double precip_probability, String precip_type, Double uv_index, Double visibility,
                           Double wind_bearing, Double wind_gust, Double wind_speed) {
            super();
            //this.sensorId = sensorId;
            //this.sensorType = sensorType;
            this.location = location;
            this.lat = lat;
            this.lon = lon;
            this.timestamp = timestamp;
            this.pressure = pressure;
            this.altitude = altitude;
            this.pressure_sealevel = pressure_sealevel;
            this.temperature = temperature;
            this.humidity = humidity;
            this.p1 = p1;
            this.p2 = p2;
            this.p0 = p0;
            this.durP1 = durP1;
            this.ratioP1 = ratioP1;
            this.durP2 = durP2;
            this.ratioP2 = ratioP2;
            this.closest_weather_station = closest_weather_station;
            this.apparent_temperature = apparent_temperature;
            this.cloud_cover = cloud_cover;
            this.dew_point = dew_point;
            this.ozone = ozone;
            this.precip_intensity = precip_intensity;
            this.precip_probability = precip_probability;
            this.precip_type = precip_type;
            this.uv_index = uv_index;
            this.visibility = visibility;
            this.wind_bearing = wind_bearing;
            this.wind_gust = wind_gust;
            this.wind_speed = wind_speed;
        }

        public JoinWeather() {
            super();
        }
    }
}

