package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.sensors.Type;
import de.tuberlin.dima.bdapro.sensors.UnifiedSensorReading;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.sql.Timestamp;

import static de.tuberlin.dima.bdapro.jobs.Filtering.readAcceptedSensors;
import static de.tuberlin.dima.bdapro.jobs.WeatherJob.readWeather;

public class Joining extends UnifiedSensorJob {
    private static String sensorBasePath = "../data/intermediate/filtered/";
    private static String filterBasePath = "../data/intermediate/";
    private static String weatherDataPath = "../data/raw/weather/weather_data.csv";
    private static boolean compressed = false;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        Path sensorDataBasePath = new Path(sensorBasePath);
        DataSet<Integer> acceptedSensors = readAcceptedSensors(env, filterBasePath);

        DataSet<Tuple5<Double, Double, Double, Double, String>> acceptedSensorData = env.readCsvFile(new Path(filterBasePath, "berlin_enrichable_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields("1111100").types(Double.class, Double.class, Double.class, Double.class, String.class);

        DataSet<UnifiedSensorReading> sensorReadings = readAllSensors(sensorDataBasePath.toString(), env);
        DataSet<UnifiedSensorReading> filteredData = filterSensors(sensorReadings, acceptedSensors);


        DataSet<WeatherReading> weatherReadingDataSet = readWeather(env, weatherDataPath, WeatherReading.class, WeatherReading.getFields());


        tEnv.registerDataSet("accepted_sensors", acceptedSensorData, "sensor_id," +
                "longitude," +
                "latitude," +
                "location," +
                "closest_weather_station"
        );

        tEnv.registerDataSet("unified_sensor_data", filteredData, "sensorId," +
                "sensorType," +
                "location," +
                "lat," +
                "lon," +
                "timestamp," +
                "pressure," +
                "altitude," +
                "pressure_sealevel," +
                "temperature," +
                "humidity," +
                "p1," +
                "p2," +
                "p0," +
                "durP1," +
                "ratioP1," +
                "durP2," +
                "ratioP2");

        tEnv.registerDataSet("weather", weatherReadingDataSet, "location," +
                "time," +
                "longitude," +
                "latitude," +
                "temperature," +
                "apparent_temperature," +
                "cloud_cover," +
                "dew_point," +
                "humidity," +
                "ozone," +
                "precip_intensity," +
                "precip_probability," +
                "precip_type," +
                "pressure," +
                "uv_index," +
                "visibility," +
                "wind_bearing," +
                "wind_gust," +
                "wind_speed");

        Table result_1 = tEnv.sqlQuery(
                "SELECT s.*,a.closest_weather_station FROM unified_sensor_data s LEFT JOIN accepted_sensors a ON s.sensorId=CAST(a.sensor_id AS int)");

        DataSet<JoinStations> joinStationsResult = tEnv.toDataSet(result_1, JoinStations.class);

        joinStationsResult.print();

        tEnv.registerDataSet("sensors_stations", joinStationsResult, "sensorId," +
                "sensorType," +
                "location," +
                "lat," +
                "lon," +
                "timestamp," +
                "pressure," +
                "altitude," +
                "pressure_sealevel," +
                "temperature," +
                "humidity," +
                "p1," +
                "p2," +
                "p0," +
                "durP1," +
                "ratioP1," +
                "durP2," +
                "ratioP2," +
                "closest_weather_station");

        StringBuilder sqlBuilder = new StringBuilder()
                .append("select a.*,w.apparent_temperature,w.cloud_cover,w.dew_point,w.ozone,w.precip_intensity, ")
                .append("w.precip_probability,w.precip_type,w.uv_index,w.visibility,w.wind_bearing,w.wind_gust,w.wind_speed ")
                .append("from sensors_stations a ")
                .append("left join weather w ")
                .append("ON (a.closest_weather_station=w.location AND EXTRACT(year FROM a.`timestamp`)=EXTRACT(year FROM w.`time`) and EXTRACT(month FROM a.`timestamp`)=EXTRACT(month FROM w.`time`) ")
                .append("AND EXTRACT(day FROM a.`timestamp`)=EXTRACT(day FROM w.`time`) AND EXTRACT(hour FROM a.`timestamp`)=EXTRACT(hour FROM w.`time`))");

        String spatio_temporal_join = sqlBuilder.toString();

        Table result = tEnv.sqlQuery(spatio_temporal_join);

        DataSet<JoinWeather> joinWeatherResult = tEnv.toDataSet(result, JoinWeather.class);

        env.execute("Joined Dataset");

    }

    public static class JoinStations {
        // Common Fields
        public Integer sensorId;
        public String sensorType;
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

        public JoinStations(Integer sensorId, String sensorType, Integer location, Double lat,Double lon,Timestamp timestamp,
                            Double pressure,Double altitude,Double pressure_sealevel,Double temperature, Double humidity,Double p1,
                            Double p2,Double p0,Double durP1,Double ratioP1,Double durP2,Double ratioP2,String closest_weather_station) {
            super();
            this.sensorId = sensorId;
            this.sensorType = sensorType;
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
        }

        public JoinStations() {
            super();
        }

        //@Override
        //public String toString() {
        //    return "StationJoin [sensorID=" + sensorId + ", sensorType=" + sensorType + ", location=" + location +
        //            ", lat=" + lat + ", lon=" + lon + ", timestamp=" + timestamp + ", pressure="+ pressure+
        //            ", altitude=" + altitude + ", pressure_sealevel=" + pressure_sealevel + ",temperature=" + temperature+
        //            ", humidity="+ humidity+ ", p1="+ p1 + ", p2=" + p2 + ", p0=" + p0 +
        //            ", durP1=" + durP1 +
        //            ", ratioP1=" + ratioP1 + ", durP2=" +durP2 + ", ratioP2=" + ratioP2 +", closest_weather_station=" + closest_weather_station +
        //            "]";
        //}

    }

    public static class JoinWeather {
        // Common Fields
        public Integer sensorId;
        public String sensorType;
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

        public JoinWeather(Integer sensorId, String sensorType, Integer location, Double lat,Double lon,Timestamp timestamp,
                           Double pressure,Double altitude,Double pressure_sealevel,Double temperature, Double humidity,Double p1,
                           Double p2,Double p0,Double durP1,Double ratioP1,Double durP2,Double ratioP2,String closest_weather_station,
                           Double apparent_temperature,Double cloud_cover,Double dew_point,Double ozone, Double precip_intensity,
                           Double precip_probability,String precip_type,Double uv_index,Double visibility,
                           Double wind_bearing,Double wind_gust, Double wind_speed) {
            super();
            this.sensorId = sensorId;
            this.sensorType = sensorType;
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

        //@Override
        //public String toString() {
        //    return "WeatherResult [sensorID=" + sensorId + ", sensorType=" + sensorType + ", location=" + location +
        //            ", lat=" + lat + ", lon=" + lon + ", timestamp=" + timestamp + ", p1=" + p1 + "durP1=" + durP1 +
        //            ", ratioP1=" + ratioP1 + ", P2=" + p2 + ", durP2=" + ", ratioP2=" + ", closest_weather_station=" +
        //            closest_weather_station + " , temperature=" + temperature +
        //            "]";
        //}
    }

    private static DataSet<UnifiedSensorReading> filterSensors(DataSet<UnifiedSensorReading> sensorData, DataSet<Integer> acceptedSensors) {
        DataSet<Tuple2<UnifiedSensorReading, Integer>> joinResult = sensorData.joinWithTiny(acceptedSensors).where((UnifiedSensorReading reading) -> reading.sensorId).equalTo((Integer sensorId) -> sensorId);
        return joinResult.map((Tuple2<UnifiedSensorReading, Integer> tuple) -> tuple.f0);
    }

    private static DataSet<UnifiedSensorReading> readAllSensors(String dataDirectory, ExecutionEnvironment env) {
        DataSet<UnifiedSensorReading> sensorReadings = null;

        for (Type sensorType : Type.values()) {
            Path sensorDataBasePath = new Path(sensorBasePath);
            DataSet<UnifiedSensorReading> sensorReading = readSensor(sensorType, sensorDataBasePath.toString(), env, compressed);
            if (sensorReadings == null) {
                sensorReadings = sensorReading;
            } else {
                sensorReadings = sensorReadings.union(sensorReading);
            }
        }
        return sensorReadings;
    }
}

