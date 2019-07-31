package de.tuberlin.dima.bdapro.dataIntegration.weather;

import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

/**
 * Test cases for the {@link Field}
 *
 * @author Ricardo Salazar
 */

public class FieldTest {

    @Test
    void testIntegerField(){
        Field field = new Field("test",Integer.class,true);
        Assertions.assertEquals(Types.INT, field.getTypeInformation());
        field.setValue("0");
        Assertions.assertEquals(0, field.getValue());
        field.setValue("-1");
        Assertions.assertEquals(-1, field.getValue());
        field.setValue("1");
        Assertions.assertEquals(1, field.getValue());
        field.setValue("null");
        Assertions.assertNull(field.getValue());
    }

    @Test
    void testDoubleField(){
        Field field = new Field("test",Double.class,true);
        Assertions.assertEquals(Types.DOUBLE, field.getTypeInformation());
        field.setValue("0");
        Assertions.assertEquals(0.0, field.getValue());
        field.setValue("-1");
        Assertions.assertEquals(-1.0, field.getValue());
        field.setValue("1");
        Assertions.assertEquals(1.0, field.getValue());
        field.setValue("1.0");
        Assertions.assertEquals(1.0, field.getValue());
        field.setValue("1.");
        Assertions.assertEquals(1.0, field.getValue());
        field.setValue("1e0");
        Assertions.assertEquals(1.0, field.getValue());
        field.setValue("1e1");
        Assertions.assertEquals(10.0, field.getValue());
        field.setValue(".1");
        Assertions.assertEquals(0.1, field.getValue());
        field.setValue("NaN");
        Assertions.assertEquals(Double.NaN, field.getValue());
        field.setValue("nan");
        Assertions.assertEquals(Double.NaN, field.getValue());
        field.setValue("null");
        Assertions.assertNull(field.getValue());
    }

    @Test
    void testTimestampField(){
        Field field = new Field("test", Timestamp.class,true);
        Assertions.assertEquals(Types.SQL_TIMESTAMP, field.getTypeInformation());
        field.setValue("2019-01-01 12:00:00");
        Assertions.assertEquals(Timestamp.valueOf("2019-01-01 12:00:00"), field.getValue());
        field.setValue("2019-01-01T12:00:00");
        Assertions.assertNull(field.getValue());
        field.setValue("");
        Assertions.assertNull(field.getValue());
        field.setValue("null");
        Assertions.assertNull(field.getValue());
    }

    @Test
    void testStringField(){
        Field field = new Field("test", String.class,true);
        Assertions.assertEquals(Types.STRING, field.getTypeInformation());
        field.setValue("0");
        Assertions.assertEquals("0", field.getValue());
        field.setValue("false");
        Assertions.assertEquals("false", field.getValue());
        field.setValue("foo");
        Assertions.assertEquals("foo", field.getValue());
        field.setValue("");
        Assertions.assertNull(field.getValue());
    }

    @Test
    void testOtherField(){
        Field field = new Field("test", Float.class,true);
        field.setValue("1.0");
        Assertions.assertNull(field.getValue());
    }


}
