package de.tuberlin.dima.bdapro.sensors;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

abstract public class SensorReading {
    public Integer sensorId;
    public String sensorType;
    public Integer location;
    public Double lat;
    public Double lon;
    public Timestamp timestamp;

    protected static List<Field> getCommonFields() {
        ArrayList<Field> fields = new ArrayList<>();
        fields.add(new Field("sensorId", Integer.class));
        fields.add(new Field("sensorType", String.class));
        fields.add(new Field("location", Integer.class));
        fields.add(new Field("lat", Double.class));
        fields.add(new Field("lon", Double.class));
        fields.add(new Field("timestamp", Timestamp.class));
        return fields;
    }
}
