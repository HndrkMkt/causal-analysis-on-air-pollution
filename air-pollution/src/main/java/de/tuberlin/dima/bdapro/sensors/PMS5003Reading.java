package de.tuberlin.dima.bdapro.sensors;

import java.util.List;

public class PMS5003Reading extends SensorReading {
    public Double p1;
    public Double p2;
    public Double p0;

    public static List<Field> getFields() {
        List<Field> fields = SensorReading.getCommonFields();
        fields.add(new Field("p1", Double.class));
        fields.add(new Field("p2", Double.class));
        fields.add(new Field("p0", Double.class));
        return fields;
    }
}
