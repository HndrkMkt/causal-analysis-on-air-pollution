package de.tuberlin.dima.bdapro.parsers;

import de.tuberlin.dima.bdapro.sensor.Field;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import de.tuberlin.dima.bdapro.sensor.Type;

/**
 * The SensorReadingParser splits input strings from the luftdaten.info data for a given sensor type and generates
 * new {@link UnifiedSensorReading}s for each input, where it sets all appropriate fields to their values and leaves
 * the rest null. The parsing logic of each individual field is specified in {@link Field}.
 *
 * @author Hendrik Makait
 */
public class SensorReadingParser implements Serializable {
    private static final String DELIMITER = ";";
    private final List<Field> fields;

    /**
     * Creates a new SensorReadingParser instance that can parse data of the given sensor type.
     *
     * @param type The type of the sensor data to parse.
     */
    public SensorReadingParser(Type type) {
        this.fields = getSensorFields(type);
    }

    /**
     * Parses the input data and returns a new {@link UnifiedSensorReading} with the appropriate fields set to their
     * values and all other fields null.
     *
     * @param input A string representing the sensor data.
     * @return a {@link UnifiedSensorReading} with the appropriate fields set to their values and all other fields null
     */
    public UnifiedSensorReading readRecord(String input) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {

        String[] tokens = input.split(DELIMITER);

        // TODO: Test on entire dataset to ensure no regression
        if (tokens.length != fields.size()) {
            throw new IllegalArgumentException("The input contains a different number of fields than the sensor type");
        }

        for (int i = 0, j = 0; i < tokens.length; j++) {
            Field curr = fields.get(j);
            curr.setValue(tokens[i++]);
        }

        UnifiedSensorReading sensorReading = new UnifiedSensorReading();

        for (Field field : fields) {
            UnifiedSensorReading.class.getField(field.getName()).set(sensorReading, field.getValue());
        }

        return sensorReading;
    }

    /**
     * Returns the list of fields for a given sensor types in the order they appear in the data.
     * This list is a subset of all the fields included in the {@link UnifiedSensorReading}.
     *
     * @param type The type of the sensor data to parse.
     * @return the list of fields for a given sensor types
     */
    private static List<Field> getSensorFields(Type type) {
        List<String> fieldNames = getSensorFieldNames(type);
        List<Field> fields = new ArrayList<>();
        Map<String, Field> fieldMap = UnifiedSensorReading.getFieldMap();
        for (String fieldName : fieldNames) {
            fields.add(fieldMap.get(fieldName));
        }
        return fields;
    }

    /**
     * Returns the list of field names that are common among all sensor types.
     *
     * @return the list of field names that are common among all sensor types
     */
    private static List<String> getCommonSensorFieldNames() {
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("sensorId");
        fieldNames.add("sensorType");
        fieldNames.add("location");
        fieldNames.add("lat");
        fieldNames.add("lon");
        fieldNames.add("timestamp");
        return fieldNames;
    }

    /**
     * Returns the list of field names for a given sensor types in the order they appear in the data.
     * This list is a subset of all the fields included in the {@link UnifiedSensorReading}.
     *
     * @param type The type of the sensor data to parse.
     * @return the list of field names for the sensor type
     */
    private static List<String> getSensorFieldNames(Type type) {
        List<String> fieldNames = getCommonSensorFieldNames();
        switch (type) {
            case BME280:
                fieldNames.add("pressure");
                fieldNames.add("altitude");
                fieldNames.add("pressure_sealevel");
                fieldNames.add("temperature");
                fieldNames.add("humidity");
                break;
            case BMP180:
                fieldNames.add("pressure");
                fieldNames.add("altitude");
                fieldNames.add("pressure_sealevel");
                fieldNames.add("temperature");
                break;
            case DHT22:
            case HTU21D:
                fieldNames.add("temperature");
                fieldNames.add("humidity");
                break;
            case DS18B20:
                fieldNames.add("temperature");
                break;
            case PMS3003:
            case PMS5003:
            case PMS7003:
                fieldNames.add("p1");
                fieldNames.add("p2");
                fieldNames.add("p0");
                break;
            case PPD42NS:
            case SDS011:
                fieldNames.add("p1");
                fieldNames.add("durP1");
                fieldNames.add("ratioP1");
                fieldNames.add("p2");
                fieldNames.add("durP2");
                fieldNames.add("ratioP2");
                break;
            case HPM:
                fieldNames.add("p1");
                fieldNames.add("p2");
                break;
            case UNIFIED:
                fieldNames.add("pressure");
                fieldNames.add("altitude");
                fieldNames.add("pressure_sealevel");
                fieldNames.add("temperature");
                fieldNames.add("humidity");
                fieldNames.add("p1");
                fieldNames.add("p2");
                fieldNames.add("p0");
                fieldNames.add("durP1");
                fieldNames.add("ratioP1");
                fieldNames.add("durP2");
                fieldNames.add("ratioP2");
                break;
        }
        return fieldNames;
    }
}
