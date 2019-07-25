package de.tuberlin.dima.bdapro.featureTable;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * The feature table is the core abstraction in the data processing component. It encapsulates all necessary data and
 * metadata to create an intermediate dataset that contains all key columns and all feature columns.
 * <p>
 * Feature tables can be joined by specifying a join predicate and referring to columns to their full names. The full
 * names should not change in nested joins when using {@link Column}.
 *
 * @author Hendrik Makait
 */
public class FeatureTable {

    private String name;
    public Table data; // TODO: Make private
    private List<Column> columns;
    private List<Column> keyColumns;

    /**
     * Creates a new feature table from the given data and metadata.
     * <p>
     * To allow for easy joining of the feature tables, the columns are renamed to <name>_<column.name>.
     *
     * @param name             The name of the feature table.
     * @param data             A Flink Table containing all relevant data for the feature table.
     * @param columns          The relevant columns of the feature table.
     * @param keyColumns       The columns forming a row key.
     * @param tableEnvironment The TableEnvironment in which the feature table shall be registered.
     */
    public FeatureTable(String name, Table data, List<? extends Column> columns, List<? extends Column> keyColumns, TableEnvironment tableEnvironment) {
        this.name = name;
        this.columns = (List<Column>) columns;
        for (Column column : columns) {
            column.setParentTable(this);
        }
        this.keyColumns = (List<Column>) keyColumns;

        StringJoiner renaming = new StringJoiner(", ");
        for (Column column : columns) {
            renaming.add(column.getName() + " as " + column.getFullName());
        }
        data = data.select(renaming.toString());
        tableEnvironment.registerTable(name, data);
        this.data = tableEnvironment.scan(name);
    }

    /**
     * Creates a new feature table by joining two existing ones.
     *
     * @param tableOne              The first table to join.
     * @param tableTwo              The second table to join.
     * @param keyColumns            The columns forming a row key for the joined feature table.
     * @param joinPredicate         The join predicate defining how to join the tables.
     * @param batchTableEnvironment The table environment in which the feature tables are registered.
     */
    public FeatureTable(FeatureTable tableOne, FeatureTable tableTwo, List<? extends Column> keyColumns, String joinPredicate, BatchTableEnvironment batchTableEnvironment) {
        Table data = batchTableEnvironment.sqlQuery("SELECT * FROM " + tableOne.getName() + " INNER JOIN " + tableTwo.getName() +
                " ON " + joinPredicate);
        this.data = data;
        this.name = data.toString();
        this.columns = new ArrayList<>(tableOne.columns);
        columns.addAll(tableTwo.columns);
        this.keyColumns = (List<Column>) keyColumns;
    }

    /**
     * Returns the name of this feature table.
     *
     * @return the name of this feature table
     */
    public String getName() {
        return name;
    }

    /**
     * Writes the key and feature columns of the feature table to a file in the given output path separated
     * by a semicolon. If the file already exists, it is overwritten.
     *
     * @param outputPath The file path of the output file including the full filename.
     * @param tEnv       The table environment to use.
     */
    public void write(Path outputPath, TableEnvironment tEnv) {
        TableSink<Row> sink = new CsvTableSink(outputPath.getPath(), ";", 1, OVERWRITE);

        ArrayList<String> columnNameList = new ArrayList<>();
        ArrayList<String> keyColumnNameList = new ArrayList<>();
        ArrayList<TypeInformation> columnTypes = new ArrayList<>();
        for (Column keyColumn : keyColumns) {
            columnNameList.add(keyColumn.getFullName());
            keyColumnNameList.add(keyColumn.getFullName());
            columnTypes.add(keyColumn.getTypeInformation());
        }

        for (Column column : columns) {
            if (column.isFeature() && !keyColumns.contains(column)) {
                columnNameList.add(column.getFullName());
                columnTypes.add(column.getTypeInformation());
            }
        }
        String[] columnNames = columnNameList.toArray(new String[columnNameList.size()]);
        String[] keyColumnNames = keyColumnNameList.toArray(new String[keyColumnNameList.size()]);

        tEnv.registerTableSink("output", columnNames, columnTypes.toArray(new TypeInformation[columnTypes.size()]), sink);
        // TODO: Add column names to file!
        data.select(StringUtils.join(columnNames, ", ")).insertInto("output");
    }

    /**
     * Creates a new feature table by joining the current table with another one.
     *
     * @param tableTwo              The table to join with.
     * @param keyColumns            The columns forming a row key for the joined feature table.
     * @param joinPredicate         The join predicate defining how to join the tables.
     * @param batchTableEnvironment The table environment in which the feature tables are registered.
     */
    public FeatureTable join(FeatureTable tableTwo, List<? extends Column> keyColumns, String joinPredicate,
                             BatchTableEnvironment batchTableEnvironment) {
        return new FeatureTable(this, tableTwo, keyColumns, joinPredicate, batchTableEnvironment);
    }

    /**
     * Returns the columns of the feature table.
     *
     * @return the columns of the feature table
     */
    public List<Column> getColumns() {
        return columns;
    }

    /**
     * Returns the key columns of the feature table.
     *
     * @return the key columns of the feature table
     */
    public List<Column> getKeyColumns() {
        return keyColumns;
    }
}
