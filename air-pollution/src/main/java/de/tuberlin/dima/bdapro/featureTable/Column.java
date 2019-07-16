package de.tuberlin.dima.bdapro.featureTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class Column extends AbstractColumn implements IColumn{
    private String name;
    private TypeInformation typeInformation;
    private boolean isFeature;

    public Column(String name, TypeInformation typeInformation, boolean isFeature) {
        this.name = name;
        this.typeInformation = typeInformation;
        this.isFeature = isFeature;
    }

    @Override
    public String getName() {
        return name;
    }

    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    public boolean isFeature() {
        return isFeature;
    }
}
