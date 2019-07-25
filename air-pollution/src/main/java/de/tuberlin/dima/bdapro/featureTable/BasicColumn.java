package de.tuberlin.dima.bdapro.featureTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * BasicColumn is a simple implementation of the {@link Column} abstract class.
 *
 * @author Hendrik Makait
 */
public class BasicColumn extends Column {
    private final String name;
    private final TypeInformation typeInformation;
    private final boolean isFeature;

    /**
     * Creates an new BasicColumn from the given input.
     *
     * @param name            The name of the column.
     * @param typeInformation The {@link TypeInformation} of the input.
     * @param isFeature       Whether the column is a feature.
     */
    public BasicColumn(String name, TypeInformation typeInformation, boolean isFeature) {
        this.name = name;
        this.typeInformation = typeInformation;
        this.isFeature = isFeature;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    @Override
    public boolean isFeature() {
        return isFeature;
    }
}
