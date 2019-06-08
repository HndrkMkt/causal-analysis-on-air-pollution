package de.tuberlin.dima.bdapro.sensors;

import org.apache.flink.api.java.tuple.Tuple12;

import java.sql.Timestamp;
import java.util.List;

public class SDS011Reading extends SensorReading {
    public Double p1;
    public Double durP1;
    public Double ratioP1;
    public Double p2;
    public Double durP2;
    public Double ratioP2;

    public static List<Field> getFields() {
        List<Field> fields = SensorReading.getCommonFields();
        fields.add(new Field("p1", Double.class));
        fields.add(new Field("durP1", Double.class));
        fields.add(new Field("ratioP1", Double.class));
        fields.add(new Field("p2", Double.class));
        fields.add(new Field("durP2", Double.class));
        fields.add(new Field("ratioP2", Double.class));
        return fields;
    }

    public Tuple12<Integer, String, Integer, Double, Double, Timestamp, Double, Double, Double, Double, Double, Double> toTuple() {
        return new Tuple12<>(
                sensorId,
                sensorType,
                location,
                lat,
                lon,
                timestamp,
                p1,
                durP1,
                ratioP1,
                p2,
                durP2,
                ratioP2);
    }
}
