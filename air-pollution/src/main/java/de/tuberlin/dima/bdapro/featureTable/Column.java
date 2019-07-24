package de.tuberlin.dima.bdapro.featureTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * TODO: Comment
 */
public abstract class Column {
    private FeatureTable parentTable;

    /**
     * Sets the parent table of the column to the feature table.
     * <p>
     * The parent table of a column should be the feature table that holds its data.
     *
     * @param featureTable The parent table of the column.
     */
    public void setParentTable(FeatureTable featureTable) {
        this.parentTable = featureTable;
    }

    /**
     * Returns the full name of the column following the pattern lt;parentTable.namegt;_lt;namegt;. This ensures
     * globally unique full names within the table environment.
     *
     * @return The full name of the column
     */
    public final String getFullName() {
        return String.format("%s_%s", parentTable.getName(), getName());
    }

    /**
     * Returns the name of the column.
     *
     * @return The name of the column
     */
    abstract public String getName();

    /**
     * Returns the type information of the column.
     *
     * @return The type information of the column
     */
    abstract public TypeInformation getTypeInformation();

    /**
     * Returns whether or not the column is a feature.
     *
     * @return Whether or not the column is a feature
     */
    abstract public boolean isFeature();
}
