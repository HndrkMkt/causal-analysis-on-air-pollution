package de.tuberlin.dima.bdapro.featureTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public interface IColumn {
    public void setParentTable(FeatureTable featureTable);

    public String getFullName();

    abstract public String getName();

    abstract public TypeInformation getTypeInformation();

    abstract public boolean isFeature();
}
