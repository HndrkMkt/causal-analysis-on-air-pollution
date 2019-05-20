package de.tuberlin.dima.bdapro.sensors;

import java.sql.Timestamp;

public class SensorStatistic {
    public int sensorId;
    public String sensorType;
    public int location;
    public Double lat;
    public Double lon;
    public Timestamp minTimestamp;
    public Timestamp maxTimestamp;
    public long readingCount;

    public SensorStatistic() {}

    public SensorStatistic(
            int sensorId,
            String sensorType,
            int location,
            Double lat,
            Double lon,
            Timestamp minTimestamp,
            Timestamp maxTimestamp,
            long readingCount
    ) {
        this.sensorId = sensorId;
        this.sensorType = sensorType;
        this.location = location;
        this.lat = lat;
        this.lon = lon;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.readingCount = readingCount;
    }
}
