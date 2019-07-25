package de.tuberlin.dima.bdapro.functions;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * Function that enables to assign timestamps to their window by rounding them up by a given time interval that
 * represents the window size.
 *
 */
public class TimeWindow extends ScalarFunction {
    private long windowInMillis;

    /**
     * Creates a new time window that rounds timestamps up to the given window size in minutes.
     *
     * @param windowInMinutes
     */
    public TimeWindow(int windowInMinutes) {
        windowInMillis = windowInMinutes * 60 * 1000;
    }

    /**
     * Rounds the given timestamp up to the timestamp that represents the end of its corresponding window.
     *
     * @param input The timestamp to round up.
     * @return The end timestamp of the corresponding window.
     */
    public Timestamp eval(Timestamp input) {
        long offset = input.getTime() % windowInMillis;
        if (offset > 0) {
            input = new Timestamp(input.getTime() + windowInMillis - offset);
        }
        return input;
    }
}
