package de.tuberlin.dima.bdapro.functions;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimeWindow extends ScalarFunction {
    private long windowInMillis;

    public TimeWindow(int windowInMinutes) {
        windowInMillis = windowInMinutes * 60 * 1000;
    }

    public Timestamp eval(Timestamp input) {
        return new Timestamp(input.getTime() - (input.getTime() % windowInMillis));
    }
}
