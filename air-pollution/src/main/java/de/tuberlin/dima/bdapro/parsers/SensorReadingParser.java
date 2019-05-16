package de.tuberlin.dima.bdapro.parsers;

import de.tuberlin.dima.bdapro.data.Field;
import de.tuberlin.dima.bdapro.data.SensorReading;

import java.io.Serializable;
import java.util.List;

public class SensorReadingParser<T extends SensorReading> implements Serializable {

    private static final String DELIMITER = ";";

    private Class<T> clazz;
    private List<Field> fields;

    public SensorReadingParser(Class<T> clazz, List<Field> fields) {
        this.clazz = clazz;
        this.fields = fields;
    }

    public T readRecord(String input) throws Exception {

        String[] tokens = input.split(DELIMITER);

        for (int i = 0, j = 0; i < tokens.length; j++) {
            Field curr = fields.get(j);
            curr.setValue(tokens[i++]);
        }

        T sensorReading = clazz.newInstance();

        try {
            for (Field field : fields) {
                clazz.getField(field.getName()).set(sensorReading, field.getValue());
            }
        } catch (Exception e) {
            throw e;
        }

        return sensorReading;
    }
}
