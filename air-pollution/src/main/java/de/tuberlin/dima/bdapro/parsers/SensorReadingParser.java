package de.tuberlin.dima.bdapro.parsers;

import de.tuberlin.dima.bdapro.sensors.Field;
import de.tuberlin.dima.bdapro.sensors.UnifiedSensorReading;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.bdapro.sensors.Type;
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

    private static List<Field> getCommonSensorFields() {
        ArrayList<Field> fields = new ArrayList<>();
        fields.add(new Field("sensorId", Integer.class));
        fields.add(new Field("sensorType", String.class));
        fields.add(new Field("location", Integer.class));
        fields.add(new Field("lat", Double.class));
        fields.add(new Field("lon", Double.class));
        fields.add(new Field("timestamp", Timestamp.class));
        return fields;
    }

    private static List<Field> getSensorFields(Type type) {
        List<Field> fields = getCommonSensorFields();
        switch (type) {
            case BME280:
                fields.add(new Field("pressure", Double.class));
                fields.add(new Field("altitude", Double.class));
                fields.add(new Field("pressure_sealevel", Double.class));
                fields.add(new Field("temperature", Double.class));
                fields.add(new Field("humidity", Double.class));
                break;
            case BMP180:
                fields.add(new Field("pressure", Double.class));
                fields.add(new Field("altitude", Double.class));
                fields.add(new Field("pressure_sealevel", Double.class));
                fields.add(new Field("temperature", Double.class));
                break;
            case DHT22:
            case HTU21D:
                fields.add(new Field("temperature", Double.class));
                fields.add(new Field("humidity", Double.class));
                break;
            case DS18B20:
                fields.add(new Field("temperature", Double.class));
                break;
            case PMS3003:
            case PMS5003:
            case PMS7003:
                fields.add(new Field("p1", Double.class));
                fields.add(new Field("p2", Double.class));
                fields.add(new Field("p0", Double.class));
                break;
            case PPD42NS:
            case SDS011:
                fields.add(new Field("p1", Double.class));
                fields.add(new Field("durP1", Double.class));
                fields.add(new Field("ratioP1", Double.class));
                fields.add(new Field("p2", Double.class));
                fields.add(new Field("durP2", Double.class));
                fields.add(new Field("ratioP2", Double.class));
                break;
            case HPM:
                fields.add(new Field("p1", Double.class));
                fields.add(new Field("p2", Double.class));
                break;
            case UNIFIED:
                fields.add(new Field("pressure", Double.class));
                fields.add(new Field("altitude", Double.class));
                fields.add(new Field("pressure_sealevel", Double.class));
                fields.add(new Field("temperature", Double.class));
                fields.add(new Field("humidity", Double.class));
                fields.add(new Field("p1", Double.class));
                fields.add(new Field("durP1", Double.class));
                fields.add(new Field("ratioP1", Double.class));
                fields.add(new Field("p2", Double.class));
                fields.add(new Field("durP2", Double.class));
                fields.add(new Field("ratioP2", Double.class));
                fields.add(new Field("p0", Double.class));
                break;
        }
        return fields;
    }
}
