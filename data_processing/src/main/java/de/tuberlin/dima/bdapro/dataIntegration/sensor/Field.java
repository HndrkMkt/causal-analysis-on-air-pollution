package de.tuberlin.dima.bdapro.dataIntegration.sensor;

import de.tuberlin.dima.bdapro.featureTable.Column;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * The Field class parses field content from the sensor data and extends the {@link Column} class used
 * for specifying feature tables.
 *
 * @author Hendrik Makait
 */
public class Field extends Column implements Serializable {
    private final Logger LOG = LoggerFactory.getLogger(Field.class);
    private static final String TIMESTAMP_FORMAT_STR = "yyyy-MM-dd'T'HH:mm:ss";

    private final String name;
    private final Class<?> clazz;
    private Object value;
    private final boolean isFeature;

    /**
     * Creates a new field.
     *
     * @param name      The name of the field.
     * @param clazz     The class of the field.
     * @param isFeature Whether the field is a feature of a feature table.
     */
    public Field(String name, Class<?> clazz, boolean isFeature) {
        this.name = name;
        this.clazz = clazz;
        this.isFeature = isFeature;
    }

    /**
     * Returns the name of the field.
     *
     * @return the name of the field
     */
    public String getName() {
        return name;
    }


    /**
     * Returns the assigned value of the field.
     *
     * @return the assigned value of the field
     */
    public Object getValue() {
        return value;
    }

    @Override
    public boolean isFeature() {
        return isFeature;
    }

    @Override
    public TypeInformation getTypeInformation() {
        return TypeInformation.of(clazz);
    }

    /**
     * Parses the input string and sets its value to the parsed one or null in case of any parsing errors.
     * <p>
     * On parsing errors, this method does throw an exception to be able to deal with errors in the input data.
     * Instead, it sets the value to null and writes the error to the log.
     *
     * @param str The string representation of the field's value.
     */
    public void setValue(String str) {
        if (clazz == null) {
            throw new IllegalArgumentException("Class not supplied for token " + name);
        }
        if (str == null || str.equals("")) {
            value = null;
            return;
        }
        try {
            if (str.equals("null")) {
                value = null;
            } else if (clazz.equals(Integer.class)) {
                value = Integer.parseInt(str);
            } else if (clazz.equals(Double.class)) {
                if (str.equalsIgnoreCase("NaN")) {
                    value = Double.NaN;
                } else {
                    value = Double.parseDouble(str);
                }
            } else if (clazz.equals(Timestamp.class)) {
                value = new Timestamp((new SimpleDateFormat(TIMESTAMP_FORMAT_STR)).parse(str).getTime());
            } else if (clazz.equals(Boolean.class)) {
                value = str.equals("1");
            } else if (clazz.equals(String.class)) {
                value = str;
            } else {
                value = null;
            }
        } catch (Exception ex) {
            value = null;
            LOG.error(ex.toString());
        }
    }
}
