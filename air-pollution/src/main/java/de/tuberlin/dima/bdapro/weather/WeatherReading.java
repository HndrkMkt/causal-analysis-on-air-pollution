package de.tuberlin.dima.bdapro.weather;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

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

    public static List<Field> getFields() {
        ArrayList<Field> fields = new ArrayList<>();
        fields.add(new Field("location", String.class));
        fields.add(new Field("time", Timestamp.class));
        fields.add(new Field("longitude", Double.class));
        fields.add(new Field("latitude", Double.class));
        fields.add(new Field("temperature", Double.class));
        List<Field> measurementFields = getMeasurementFields();
        fields.addAll(measurementFields);
        return fields;
    }

    public static List<Field> getMeasurementFields() {
        ArrayList<Field> fields = new ArrayList<>();
        fields.add(new Field("apparent_temperature", Double.class));
        fields.add(new Field("cloud_cover", Double.class));
        fields.add(new Field("dew_point", Double.class));
        fields.add(new Field("humidity", Double.class));
        fields.add(new Field("ozone", Double.class));
        fields.add(new Field("precip_intensity", Double.class));
        fields.add(new Field("precip_probability", Double.class));
        fields.add(new Field("precip_type", String.class));
        fields.add(new Field("pressure", Double.class));
        fields.add(new Field("uv_index", Double.class));
        fields.add(new Field("visibility", Double.class));
        fields.add(new Field("wind_bearing", Double.class));
        fields.add(new Field("wind_gust", Double.class));
        fields.add(new Field("wind_speed", Double.class));
        return fields;
    }

    public static final String[] getMeasurementFieldNames() {
        List<String> measurementFieldNames = new ArrayList<>();
        for (Field field : getMeasurementFields()) {
            measurementFieldNames.add(field.getName());
        }
        String[] result = new String[measurementFieldNames.size()];
        return measurementFieldNames.toArray(result);
    }

    public static final TypeInformation[] getMeasurementFieldTypes() {
        List<TypeInformation> measurementFieldTypes = new ArrayList<>();
        for (Field field : getMeasurementFields()) {
            measurementFieldTypes.add(field.getType());
        }
        TypeInformation[] result = new TypeInformation[measurementFieldTypes.size()];
        return measurementFieldTypes.toArray(result);
    }
}
