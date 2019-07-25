package de.tuberlin.dima.bdapro.functions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

public class TimeWindowTest {
    @Test
    void testTimeWindowing() {
        Timestamp expected1 = Timestamp.valueOf("2019-01-01 00:00:00");
        TimeWindow timeWindow60mins = new TimeWindow(60);
        Assertions.assertEquals(expected1,
                timeWindow60mins.eval(Timestamp.valueOf("2018-12-31 23:59:59")));
        Assertions.assertEquals(expected1, timeWindow60mins.eval(Timestamp.valueOf("2019-01-01 00:00:00")));

        Timestamp expected2 = Timestamp.valueOf("2019-01-01 01:00:00");
        Assertions.assertEquals(expected2, timeWindow60mins.eval(Timestamp.valueOf("2019-01-01 00:00:01")));
        Assertions.assertEquals(expected2, timeWindow60mins.eval(Timestamp.valueOf("2019-01-01 00:59:59")));
        Assertions.assertEquals(expected2,
                timeWindow60mins.eval(Timestamp.valueOf("2019-01-01 01:00:00")));
    }
}
