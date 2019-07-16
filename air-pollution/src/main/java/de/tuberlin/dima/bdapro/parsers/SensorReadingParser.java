package de.tuberlin.dima.bdapro.parsers;

import de.tuberlin.dima.bdapro.sensor.Field;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import de.tuberlin.dima.bdapro.sensor.Type;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SensorReadingParser implements Serializable {
    Logger LOG = LoggerFactory.getLogger(SensorReadingParser.class);
    private static final String DELIMITER = ";";
    private List<Field> fields;

    public SensorReadingParser(Type type) {
        this.fields = getSensorFields(type);
    }

    public UnifiedSensorReading readRecord(String input) throws Exception {

        String[] tokens = input.split(DELIMITER);

        for (int i = 0, j = 0; i < tokens.length; j++) {
            Field curr = fields.get(j);
            curr.setValue(tokens[i++]);
        }

        UnifiedSensorReading sensorReading = new UnifiedSensorReading();

        try {
            for (Field field : fields) {
                UnifiedSensorReading.class.getField(field.getName()).set(sensorReading, field.getValue());
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }

        return sensorReading;
    }

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

    private static List<Field> getSensorFields(Type type) {
        List<String> fieldNames  = getSensorFieldNames(type);
        List<Field> fields = new ArrayList<>();
        Map<String, Field> fieldMap = UnifiedSensorReading.getFieldMap();
        for (String fieldName : fieldNames) {
            fields.add(fieldMap.get(fieldName));
        }
        return fields;
    }

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
                fieldNames.add("durP1");
                fieldNames.add("ratioP1");
                fieldNames.add("p2");
                fieldNames.add("durP2");
                fieldNames.add("ratioP2");
                fieldNames.add("p0");
                break;
        }
        return fieldNames;
    }
}
