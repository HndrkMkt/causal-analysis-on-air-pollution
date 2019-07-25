package de.tuberlin.dima.bdapro.parsers;

import de.tuberlin.dima.bdapro.weather.Field;
import de.tuberlin.dima.bdapro.weather.WeatherReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * The WeatherReadingParser splits data from the weather input data from OpenWeatherMap web APIs and generates {@link WeatherReading}.
 * It sets all valid features to their appropriate values and leaves the rest null. The logic of each field is specified in {@link Field}
 *
 * @author Ricardo Salazar
 */
public class WeatherReadingParser implements Serializable {
    Logger LOG = LoggerFactory.getLogger(WeatherReadingParser.class);
    private static final String DELIMITER = ";";

    private List<Field> fields;

    /**
     * Creates a new WeatherReadingParser instance that can parse data from the input weather data fields
     *
     * @param fields The fields of the weather input data
     */
    public WeatherReadingParser(List<Field> fields) {
        this.fields = fields;
    }

    /**
     *Parses the weather input data and returns a new {@link WeatherReading} with the corresponding valid fields set to their values
     * and all other fields set to null.
     *
     * @param input
     * @return a {@link WeatherReading} with the corresponding valid fields set to their values and all other fields set to null
     * @throws Exception catch exception when a a field from the input data does not exist in the WeatherReading class
     */
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
