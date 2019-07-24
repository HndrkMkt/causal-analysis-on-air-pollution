package de.tuberlin.dima.bdapro.featureTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * BasicColumn is a simple implenmentation of the IColumn interface
 * <p>
 * TODO: Write comment
 *
 * @author Hendrik Makait
 */
public class BasicColumn extends Column {
    private String name;
    private TypeInformation typeInformation;
    private boolean isFeature;

    /**
     * TODO: Comment
     *
     * @param name
     * @param typeInformation
     * @param isFeature
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

    /**
     * TODO: Comment
     *
     * @return
     */
    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    /**
     * TODO: Comment
     *
     * @return
     */
    public boolean isFeature() {
        return isFeature;
    }
}
