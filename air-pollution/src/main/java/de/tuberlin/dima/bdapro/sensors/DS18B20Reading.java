package de.tuberlin.dima.bdapro.sensors;

import java.util.List;

public class DS18B20Reading extends SensorReading {
    public Double temperature;

    public static List<Field> getFields() {
        List<Field> fields = SensorReading.getCommonFields();
        fields.add(new Field("temperature", Double.class));
        return fields;
    }
}