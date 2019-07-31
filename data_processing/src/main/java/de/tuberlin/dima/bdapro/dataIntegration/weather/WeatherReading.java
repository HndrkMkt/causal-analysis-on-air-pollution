package de.tuberlin.dima.bdapro.dataIntegration.weather;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class is a representation of the data variables available in the OpenWeatherMap web APIs
 *
 * @author Ricardo Salazar
 */
public class WeatherReading {
    public String location;
    public Timestamp time;
    public double longitude;
    public double latitude;
    public double temperature;
    public double apparent_temperature;
    public double cloud_cover;
    public double dew_point;
    public double humidity;
    public double ozone;
    public double precip_intensity;
    public double precip_probability;
    public String precip_type;
    public double pressure;
    public double uv_index;
    public double visibility;
    public double wind_bearing;
    public double wind_gust;
    public double wind_speed;

    /**
     * Returns a list that consists of the corresponding {@link Field} instances for each attribute of the class.
     *
     * @return a list that consists of the corresponding {@link Field} instances for each attribute of the class.
     */
    public static List<Field> getFields() {
        ArrayList<Field> fields = new ArrayList<>();
        fields.add(new Field("location", String.class, false));
        fields.add(new Field("time", Timestamp.class, false));
        fields.add(new Field("longitude", Double.class, false));
        fields.add(new Field("latitude", Double.class, false));
        fields.add(new Field("temperature", Double.class, false));
        fields.add(new Field("apparent_temperature", Double.class, true));
        fields.add(new Field("cloud_cover", Double.class, true));
        fields.add(new Field("dew_point", Double.class, true));
        fields.add(new Field("humidity", Double.class, true));
        fields.add(new Field("ozone", Double.class, true));
        fields.add(new Field("precip_intensity", Double.class, true));
        fields.add(new Field("precip_probability", Double.class, true));
        fields.add(new Field("precip_type", String.class, true));
        fields.add(new Field("pressure", Double.class, true));
        fields.add(new Field("uv_index", Double.class, true));
        fields.add(new Field("visibility", Double.class, true));
        fields.add(new Field("wind_bearing", Double.class, true));
        fields.add(new Field("wind_gust", Double.class, true));
        fields.add(new Field("wind_speed", Double.class, true));
        return fields;
    }


    @Override
    public boolean equals(Object o) {

        if (o == this) return true;
        if (!(o instanceof WeatherReading)) {
            return false;
        }
        WeatherReading reading = (WeatherReading) o;
        return Objects.equals(location, reading.location) &&
                Objects.equals(time, reading.time) &&
                Objects.equals(longitude, reading.longitude) &&
                Objects.equals(latitude, reading.latitude) &&
                Objects.equals(temperature, reading.temperature) &&
                Objects.equals(apparent_temperature, reading.apparent_temperature) &&
                Objects.equals(cloud_cover, reading.cloud_cover) &&
                Objects.equals(dew_point, reading.dew_point) &&
                Objects.equals(humidity, reading.humidity) &&
                Objects.equals(ozone, reading.ozone) &&
                Objects.equals(precip_intensity, reading.precip_intensity) &&
                Objects.equals(precip_probability, reading.precip_probability) &&
                Objects.equals(precip_type, reading.precip_type) &&
                Objects.equals(pressure, reading.pressure) &&
                Objects.equals(uv_index, reading.uv_index) &&
                Objects.equals(visibility, reading.visibility) &&
                Objects.equals(wind_bearing, reading.wind_bearing) &&
                Objects.equals(wind_gust, reading.wind_gust) &&
                Objects.equals(wind_speed, reading.wind_speed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location,time,longitude,latitude,temperature,apparent_temperature,cloud_cover,dew_point,humidity,ozone,
                precip_intensity,precip_probability,precip_type,pressure,uv_index,visibility,wind_bearing,wind_gust,wind_speed);
    }
}
