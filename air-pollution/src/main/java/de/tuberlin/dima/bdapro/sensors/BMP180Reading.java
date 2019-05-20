package de.tuberlin.dima.bdapro.sensors;

import java.util.List;

public class BMP180Reading extends SensorReading {
    public Double pressure;
    public Double altitude;
    public Double pressure_sealevel;
    public Double temperature;

    public static List<Field> getFields() {
        List<Field> fields = SensorReading.getCommonFields();
        fields.add(new Field("pressure", Double.class));
        fields.add(new Field("altitude", Double.class));
        fields.add(new Field("pressure_sealevel", Double.class));
        fields.add(new Field("temperature", Double.class));
        return fields;
    }
}
