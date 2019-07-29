package de.tuberlin.dima.bdapro.sensor;

import org.apache.flink.api.java.tuple.*;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class is a unified representation of the different sensor types encountered in the luftdaten.info data.
 * <p>
 * A unified sensor reading contains all possible fields for the different sensor types and fields that do not belong to
 * a certain sensor type can be null. The unified format allows for easy merging and aggregation over
 * multiple different sensors.
 *
 * @author Hendrik Makait
 */
public class UnifiedSensorReading {
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

    // All fields that are used to aggregate sensor readings in order to reduce noise
    public static final String[] AGGREGATION_FIELDS = {"pressure", "altitude", "pressure_sealevel", "temperature", "humidity",
            "p1", "p2", "p0", "durP1", "ratioP1", "durP2", "ratioP2"};

    /**
     * Returns a tuple representation of this instance.
     *
     * @return a tuple representation of this instance
     */
    public Tuple18<Integer, String, Integer, Double, Double, Timestamp,
            Double, Double, Double, Double, Double,
            Double, Double, Double, Double, Double, Double, Double> toTuple() {
        return new Tuple18<>(
                // Common Fields
                sensorId,
                sensorType,
                location,
                lat,
                lon,
                timestamp,

                // Sensor specific fields
                pressure,
                altitude,
                pressure_sealevel,
                temperature,
                humidity,

                p1,
                p2,
                p0,
                durP1,
                ratioP1,
                durP2,
                ratioP2);
    }

    /**
     * Returns a String object that representing the value of all variables of this UnifiedSensorReading instance that
     * belong to the given sensor type. The values are semicolon-separated.
     * <p>
     * If any other type than UNIFIED is given, the String contains only a subset of the fields in the order they appear
     * in the luftdaten.info data.
     *
     * @param type The sensor type this instance represents.
     * @return String representation of this instance
     */
    public String toString(Type type) {
        String result;
        switch (type) {
            case BME280:
                result = commonFieldsToString() + ";" +
                        pressure + ";" +
                        altitude + ";" +
                        pressure_sealevel + ";" +
                        temperature + ";" +
                        humidity;
                break;
            case BMP180:
                result = commonFieldsToString() + ";" +
                        pressure + ";" +
                        altitude + ";" +
                        pressure_sealevel + ";" +
                        temperature;
                break;
            case DHT22:
            case HTU21D:
                result = commonFieldsToString() + ";" +
                        temperature + ";" +
                        humidity;
                break;
            case DS18B20:
                result = commonFieldsToString() + ";" +
                        temperature;
                break;
            case HPM:
                result = commonFieldsToString() + ";" +
                        p1 + ";" +
                        p2;
                break;
            case PMS3003:
            case PMS5003:
            case PMS7003:
                result = commonFieldsToString() + ";" +
                        p1 + ";" +
                        p2 + ";" +
                        p0;
                break;
            case PPD42NS:
            case SDS011:
                result = commonFieldsToString() + ";" +
                        p1 + ";" +
                        durP1 + ";" +
                        ratioP1 + ";" +
                        p2 + ";" +
                        durP2 + ";" +
                        ratioP2;
                break;
            case UNIFIED:
            default:
                result = commonFieldsToString() + ";" +
                        // Sensor specific fields
                        pressure + ";" +
                        altitude + ";" +
                        pressure_sealevel + ";" +
                        temperature + ";" +
                        humidity + ";" +

                        p1 + ";" +
                        p2 + ";" +
                        p0 + ";" +
                        durP1 + ";" +
                        ratioP1 + ";" +
                        durP2 + ";" +
                        ratioP2;
                break;
        }
        return result;
    }

    /**
     * Returns a String object that representing the value of all variables of this UnifiedSensorReading instance that
     * are common among all sensor types. The values are semicolon-separated.
     *
     * @return a String representation of all common values
     */
    private String commonFieldsToString() {
        return sensorId + ";" +
                sensorType + ";" +
                location + ";" +
                lat + ";" +
                lon + ";" +
                (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")).format(timestamp);
    }

    /**
     * Returns a list that consists of the corresponding {@link Field} instances for each attribute of the class.
     *
     * @return a list that consists of the corresponding {@link Field} instances for each attribute of the class
     */
    public static List<Field> getFields() {
        ArrayList<Field> fields = new ArrayList<>();
        fields.add(new Field("sensorId", Integer.class, false));
        fields.add(new Field("sensorType", String.class, false));
        fields.add(new Field("location", Integer.class, false));
        fields.add(new Field("lat", Double.class, false));
        fields.add(new Field("lon", Double.class, false));
        fields.add(new Field("timestamp", Timestamp.class, false));

        // Sensor specific fields
        fields.add(new Field("pressure", Double.class, true));
        fields.add(new Field("altitude", Double.class, false));
        fields.add(new Field("pressure_sealevel", Double.class, true));
        fields.add(new Field("temperature", Double.class, true));
        fields.add(new Field("humidity", Double.class, true));

        fields.add(new Field("p1", Double.class, true));
        fields.add(new Field("p2", Double.class, true));
        fields.add(new Field("p0", Double.class, true));
        fields.add(new Field("durP1", Double.class, true));
        fields.add(new Field("ratioP1", Double.class, true));
        fields.add(new Field("durP2", Double.class, true));
        fields.add(new Field("ratioP2", Double.class, true));

        return fields;
    }

    /**
     * Returns a map of attribute names to {@link Field} instances for all attributes of the class.
     *
     * @return a map of attribute names to {@link Field} instances for all attributes of the class
     */
    public static Map<String, Field> getFieldMap() {
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field field : getFields()) {
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) return true;
        if (!(o instanceof UnifiedSensorReading)) {
            return false;
        }
        UnifiedSensorReading reading = (UnifiedSensorReading) o;
        return Objects.equals(sensorId, reading.sensorId) &&
                Objects.equals(sensorType, reading.sensorType) &&
                Objects.equals(location, reading.location) &&
                Objects.equals(lat, reading.lat) &&
                Objects.equals(lon, reading.lon) &&
                Objects.equals(timestamp, reading.timestamp) &&
                Objects.equals(pressure, reading.pressure) &&
                Objects.equals(altitude, reading.altitude) &&
                Objects.equals(pressure_sealevel, reading.pressure_sealevel) &&
                Objects.equals(temperature, reading.temperature) &&
                Objects.equals(humidity, reading.humidity) &&
                Objects.equals(p1, reading.p1) &&
                Objects.equals(p2, reading.p2) &&
                Objects.equals(p0, reading.p0) &&
                Objects.equals(durP1, reading.durP1) &&
                Objects.equals(ratioP1, reading.ratioP1) &&
                Objects.equals(durP2, reading.durP2) &&
                Objects.equals(ratioP2, reading.ratioP2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, sensorType, location, lat, lon, timestamp, pressure, altitude, pressure_sealevel,
                temperature, humidity, p1, p2, p0, durP1, ratioP1, durP2, ratioP2);
    }
}