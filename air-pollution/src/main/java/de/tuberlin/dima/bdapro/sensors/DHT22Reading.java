package de.tuberlin.dima.bdapro.sensors;

import org.apache.flink.api.java.tuple.Tuple8;

import java.sql.Timestamp;
import java.util.List;

public class DHT22Reading extends SensorReading {
    public Double temperature;
    public Double humidity;

    public static List<Field> getFields() {
        List<Field> fields = SensorReading.getCommonFields();
        fields.add(new Field("temperature", Double.class));
        fields.add(new Field("humidity", Double.class));
        return fields;
    }

    public Tuple8<Integer, String, Integer, Double, Double, Timestamp, Double, Double> toTuple() {
        return new Tuple8<>(
                sensorId,
                sensorType,
                location,
                lat,
                lon,
                timestamp,
                temperature,
                humidity);
    }
}
