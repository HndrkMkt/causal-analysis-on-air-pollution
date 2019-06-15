package de.tuberlin.dima.bdapro.sensors;

import org.apache.flink.api.java.tuple.*;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

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

    public static final String[] AGGREGATION_FIELDS = {"pressure", "altitude", "pressure_sealevel", "temperature", "humidity",
            "p1", "p2", "p0", "durP1", "ratioP1", "durP2", "ratioP2"};

    public Tuple18<Integer, String, Integer, Double, Double, Timestamp,
            Double, Double, Double, Double, Double,
            Double, Double, Double, Double, Double, Double, Double> toTuple() {
        return new Tuple18<>(
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
    }

    public String toString(Type type) {
        String result;
        switch (type) {
            case BME280:
                result = commonFieldsToString() + ";" +
                        pressure + ";" +
                        altitude + ";" +
                        pressure_sealevel + ";" +
                        temperature + ";" +
                        humidity;
                break;
            case BMP180:
                result = commonFieldsToString() + ";" +
                        pressure + ";" +
                        altitude + ";" +
                        pressure_sealevel + ";" +
                        temperature;
                break;
            case DHT22:
            case HTU21D:
                result = commonFieldsToString() + ";" +
                        temperature + ";" +
                        humidity;
                break;
            case DS18B20:
                result = commonFieldsToString() + ";" +
                        temperature;
                break;
            case HPM:
                result = commonFieldsToString() + ";" +
                        p1 + ";" +
                        p2;
                break;
            case PMS3003:
            case PMS5003:
            case PMS7003:
                result = commonFieldsToString() + ";" +
                        p1 + ";" +
                        p2 + ";" +
                        p0;
                break;
            case PPD42NS:
            case SDS011:
                result = commonFieldsToString() + ";" +
                        p1 + ";" +
                        durP1 + ";" +
                        ratioP1 + ";" +
                        p2 + ";" +
                        durP2 + ";" +
                        ratioP2;
                break;
            case UNIFIED:
            default:
                result = commonFieldsToString() + ";" +
                        // Sensor specific fields
                        pressure + ";" +
                        altitude + ";" +
                        pressure_sealevel + ";" +
                        temperature + ";" +
                        humidity + ";" +

                        p1 + ";" +
                        p2 + ";" +
                        p0 + ";" +
                        durP1 + ";" +
                        ratioP1 + ";" +
                        durP2 + ";" +
                        ratioP2;
                break;
        }
        return result;
    }

    private String commonFieldsToString() {
        return sensorId + ";" +
                sensorType + ";" +
                location + ";" +
                lat + ";" +
                lon + ";" +
                (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")).format(timestamp);
    }
}