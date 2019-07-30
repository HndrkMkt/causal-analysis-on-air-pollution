package de.tuberlin.dima.bdapro.dataIntegration.weather;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is a representation of the data variables available in the OpenWeatherMap web APIs
 *
 * @author Ricardo Salazar
 */
public class WeatherReading {

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
}
