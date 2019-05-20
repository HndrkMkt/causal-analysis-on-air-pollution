package de.tuberlin.dima.bdapro.sensors;

import java.util.List;

public class HTU21DReading extends SensorReading {
    public Double temperature;
    public Double humidity;

    public static List<Field> getFields() {
        List<Field> fields = SensorReading.getCommonFields();
        fields.add(new Field("temperature", Double.class));
        fields.add(new Field("humidity", Double.class));
        return fields;
    }
}
