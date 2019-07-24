package de.tuberlin.dima.bdapro.weather;

import de.tuberlin.dima.bdapro.featureTable.Column;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class Field extends Column implements Serializable {
    Logger LOG = LoggerFactory.getLogger(Field.class);
    private static final String TIMESTAMP_FORMATSTR = "yyyy-MM-dd HH:mm:ss";

    private String name;
    private Class<?> clazz;
    private Object value;
    private boolean isFeature;

    public Field(String name, Class<?> clazz, boolean isFeature) {
        this.name = name;
        this.clazz = clazz;
        this.isFeature = isFeature;
    }

    public String getName() {
        return name;
    }

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

    public void setValue(String str) throws Exception {
        if (clazz == null) {
            throw new IllegalArgumentException("Class not supplied for token " + name);
        }
        if (str == null || str.equals("")) {
            if (clazz.equals(Double.class)) {
                value = Double.NaN;
                return;
            } else if (clazz.equals(String.class)){
                value = null;
                return;
            }
        }
        try {
            if (clazz.equals(Integer.class)) {
                value = Integer.parseInt(str);
            } else if (clazz.equals(Double.class)) {
                if (str.equalsIgnoreCase("NaN")) {
                    value = Double.NaN;
                } else {
                    value = Double.parseDouble(str);
                }
            } else if (clazz.equals(Timestamp.class)) {
                value = new Timestamp((new SimpleDateFormat(TIMESTAMP_FORMATSTR)).parse(str).getTime());
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
