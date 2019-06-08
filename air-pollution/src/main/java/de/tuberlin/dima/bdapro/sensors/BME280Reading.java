package de.tuberlin.dima.bdapro.sensors;

import org.apache.flink.api.java.tuple.Tuple11;

import java.sql.Timestamp;
import java.util.List;

public class BME280Reading extends SensorReading {
    public String pressure;
    public Double altitude;
    public Double pressure_sealevel;
    public Double temperature;
    public Double humidity;

    public static List<Field> getFields() {
        List<Field> fields = SensorReading.getCommonFields();
        fields.add(new Field("pressure", String.class));
        fields.add(new Field("altitude", Double.class));
        fields.add(new Field("pressure_sealevel", Double.class));
        fields.add(new Field("temperature", Double.class));
        fields.add(new Field("humidity", Double.class));
        return fields;
    }

    public Tuple11<Integer, String, Integer, Double, Double, Timestamp, String, Double, Double, Double, Double> toTuple() {
        return new Tuple11<>(
                sensorId,
                sensorType,
                location,
                lat,
                lon,
                timestamp,
                pressure,
                altitude,
                pressure_sealevel,
                temperature,
                humidity);
    }
}
