package de.tuberlin.dima.bdapro.parsers;

import de.tuberlin.dima.bdapro.weather.Field;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class WeatherReadingParser implements Serializable {
    Logger LOG = LoggerFactory.getLogger(WeatherReadingParser.class);
    private static final String DELIMITER = ";";

    private List<Field> fields;

    public WeatherReadingParser(List<Field> fields) {
        this.fields = fields;
    }

    public WeatherReading readRecord(String input) throws Exception {

        String[] tokens = input.split(DELIMITER);

        for (int i = 0, j = 0; i < tokens.length; j++) {
            Field curr = fields.get(j);
            curr.setValue(tokens[i++]);
        }

        WeatherReading weatherReading = new WeatherReading();

        try {
            for (Field field : fields) {
                WeatherReading.class.getField(field.getName()).set(weatherReading, field.getValue());
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }

        return weatherReading;
    }
}
