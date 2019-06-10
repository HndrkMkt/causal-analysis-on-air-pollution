package de.tuberlin.dima.bdapro.sensors;

import org.apache.flink.api.java.tuple.*;

import java.sql.Timestamp;

public class UnifiedSensorReading {
    // Common Fields
    public Integer sensorId;
    public String sensorType;
    public Integer location;
    public Double lat;
    public Double lon;
    public Timestamp timestamp;

    // Sensor specific fields
    public Double pressure;
    public Double altitude;
    public Double pressure_sealevel;
    public Double temperature;
    public Double humidity;

    public Double p1;
    public Double p2;
    public Double p0;
    public Double durP1;
    public Double ratioP1;
    public Double durP2;
    public Double ratioP2;

    public Tuple toTuple(Type type) {
        Tuple result;
        switch (type) {
            case BME280:
                result = new Tuple11<>(
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
                break;
            case BMP180:
                result = new Tuple10<>(
                        sensorId,
                        sensorType,
                        location,
                        lat,
                        lon,
                        timestamp,
                        pressure,
                        altitude,
                        pressure_sealevel,
                        temperature);
                break;
            case DHT22:
            case HTU21D:
                result = new Tuple8<>(
                        sensorId,
                        sensorType,
                        location,
                        lat,
                        lon,
                        timestamp,
                        temperature,
                        humidity);
                break;
            case DS18B20:
                result = new Tuple7<>(
                        sensorId,
                        sensorType,
                        location,
                        lat,
                        lon,
                        timestamp,
                        temperature);
                break;
            case HPM:
                result = new Tuple8<>(
                        sensorId,
                        sensorType,
                        location,
                        lat,
                        lon,
                        timestamp,
                        p1,
                        p2);
                break;
            case PMS3003:
            case PMS5003:
            case PMS7003:
                result = new Tuple9<>(
                        sensorId,
                        sensorType,
                        location,
                        lat,
                        lon,
                        timestamp,
                        p1,
                        p2,
                        p0);
                break;
            case PPD42NS:
            case SDS011:
                result = new Tuple12<>(
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
                break;
            case UNIFIED:
            default:
                result = new Tuple18<>(
                        // Common Fields
                        sensorId,
                        sensorType,
                        location,
                        lat,
                        lon,
                        timestamp,

                        // Sensor specific fields
                        pressure,
                        altitude,
                        pressure_sealevel,
                        temperature,
                        humidity,

                        p1,
                        p2,
                        p0,
                        durP1,
                        ratioP1,
                        durP2,
                        ratioP2);
                break;
        }
        return result;
    }
}
