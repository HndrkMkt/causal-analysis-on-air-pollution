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
 * TODO: Comment
 *
 * @author Hendrik Makait
 */
public class FeatureTable {

    private String name;
    public Table data;
    private List<IColumn> columns;
    private List<IColumn> keyColumns;


    /**
     * Creates a new feature table from the given data and metadata.
     *
     * To allow for easy joining of the feature tables, the columns are renamed to <name>_<column.name>.
     *
     * @param name The name of the feature table.
     * @param data A Flink Table containing all relevant data for the feature table.
     * @param columns The relevant columns of the feature table.
     * @param keyColumns The columns forming a row key.
     * @param tableEnvironment The TableEnvironment in which the feature table shall be registered.
     */
    public FeatureTable(String name, Table data, List<? extends IColumn> columns, List<? extends IColumn> keyColumns, TableEnvironment tableEnvironment) {
        this.name = name;
        this.columns = (List<IColumn>) columns;
        for (IColumn column : columns) {
            column.setParentTable(this);
        }
        this.keyColumns = (List<IColumn>) keyColumns;

        StringJoiner renaming = new StringJoiner(", ");
        for (IColumn column : columns) {
            renaming.add(column.getName() + " as " + column.getFullName());
        }
        data = data.select(renaming.toString());
        tableEnvironment.registerTable(name, data);
        this.data = tableEnvironment.scan(name);
    }

    /**
     * Creates a new feature table by joining two existing ones.
     *
     * @param tableOne The first table to join.
     * @param tableTwo The second table to join.
     * @param keyColumns The columns forming a row key for the joined feature table.
     * @param joinPredicate The join predicate defining how to join the tables.
     * @param batchTableEnvironment The table environment in which the feature tables are registered.
     */
    public FeatureTable(FeatureTable tableOne, FeatureTable tableTwo, List<? extends IColumn> keyColumns, String joinPredicate, BatchTableEnvironment batchTableEnvironment) {
        Table data = batchTableEnvironment.sqlQuery("SELECT * FROM " + tableOne.getName() + " INNER JOIN " + tableTwo.getName() +
                " ON " + joinPredicate);
        this.data = data;
        this.name = data.toString();
        this.columns = new ArrayList<>(tableOne.columns);
        columns.addAll(tableTwo.columns);
        this.keyColumns = (List<IColumn>) keyColumns;
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
     * TODO: Write comment
     *
     * @param outputPath
     * @param tEnv
     */
    public void write(Path outputPath, TableEnvironment tEnv) {
        TableSink<Row> sink = new CsvTableSink(outputPath.getPath(), ";", 1, OVERWRITE);

        ArrayList<String> columnNameList = new ArrayList<>();
        ArrayList<TypeInformation> columnTypes = new ArrayList<>();
        for (IColumn keyColumn : keyColumns) {
            columnNameList.add(keyColumn.getFullName());
            columnTypes.add(keyColumn.getTypeInformation());
        }

        for (IColumn column : columns) {
            if (column.isFeature() && !keyColumns.contains(column)) {
                columnNameList.add(column.getFullName());
                columnTypes.add(column.getTypeInformation());
            }
        }
        String[] columnNames = columnNameList.toArray(new String[columnNameList.size()]);

        tEnv.registerTableSink("output", columnNames, columnTypes.toArray(new TypeInformation[columnTypes.size()]), sink);
        data.select(StringUtils.join(columnNames, ", ")).insertInto("output");
    }

    /**
     * Creates a new feature table by joining the current table with another one.
     *
     * @param tableTwo The table to join with.
     * @param keyColumns The columns forming a row key for the joined feature table.
     * @param joinPredicate The join predicate defining how to join the tables.
     * @param batchTableEnvironment The table environment in which the feature tables are registered.
     */
    public FeatureTable join(FeatureTable tableTwo, List<? extends IColumn> keyColumns, String joinPredicate,
                             BatchTableEnvironment batchTableEnvironment) {
        return new FeatureTable(this, tableTwo, keyColumns, joinPredicate, batchTableEnvironment);
    }

    /**
     * Returns the key columns of the feature table.
     *
     * @return the key columns of the feature table
     */
    public List<IColumn> getKeyColumns() {
        return keyColumns;
    }
}
