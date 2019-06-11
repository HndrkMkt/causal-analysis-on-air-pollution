package de.tuberlin.dima.bdapro;

import de.tuberlin.dima.bdapro.sensors.Type;
import de.tuberlin.dima.bdapro.sensors.UnifiedSensorReading;
import org.apache.flink.api.java.io.TextOutputFormat;

public class SensorReadingFormatter implements TextOutputFormat.TextFormatter<UnifiedSensorReading> {
    private Type sensorType;

    public SensorReadingFormatter(Type sensorType) {
        this.sensorType = sensorType;
    }

    @Override
    public String format(UnifiedSensorReading value) {
        return value.toString(sensorType);
    }
}
