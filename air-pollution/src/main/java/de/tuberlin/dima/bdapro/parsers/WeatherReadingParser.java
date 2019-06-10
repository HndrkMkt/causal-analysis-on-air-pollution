package de.tuberlin.dima.bdapro.parsers;

import de.tuberlin.dima.bdapro.weather.Field;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class WeatherReadingParser<T extends WeatherReading> implements Serializable {
    Logger LOG = LoggerFactory.getLogger(WeatherReadingParser.class);
    private static final String DELIMITER = ";";

    private Class<T> clazz;
    private List<Field> fields;

    public WeatherReadingParser(Class<T> clazz, List<Field> fields) {
        this.clazz = clazz;
        this.fields = fields;
    }

    public T readRecord(String input) throws Exception {

        String[] tokens = input.split(DELIMITER);

        for (int i = 0, j = 0; i < tokens.length; j++) {
            Field curr = fields.get(j);
            curr.setValue(tokens[i++]);
        }

        T Weather = clazz.newInstance();

        try {
            for (Field field : fields) {
                clazz.getField(field.getName()).set(Weather, field.getValue());
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }

        return Weather;
    }
}
