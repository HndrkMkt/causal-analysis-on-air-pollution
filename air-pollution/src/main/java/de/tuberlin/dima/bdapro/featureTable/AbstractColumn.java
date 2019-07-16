package de.tuberlin.dima.bdapro.featureTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public abstract class AbstractColumn implements IColumn {
    private FeatureTable parentTable;

    public void setParentTable(FeatureTable featureTable) {
        this.parentTable = featureTable;
    }

    public String getFullName() {
        return String.format("%s_%s", parentTable.getName(), getName());
    }

    abstract public String getName();

    abstract public TypeInformation getTypeInformation();

    abstract public boolean isFeature();
}
