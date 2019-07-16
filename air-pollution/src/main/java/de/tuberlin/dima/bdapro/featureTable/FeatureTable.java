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

public class FeatureTable {
    private String name;
    public Table data;
    private List<IColumn> columns;
    private List<IColumn> keyColumns;

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

    public FeatureTable(FeatureTable tableOne, FeatureTable tableTwo, List<? extends IColumn> keyColumns, String joinPredicate, BatchTableEnvironment batchTableEnvironment) {
        Table data = batchTableEnvironment.sqlQuery("SELECT * FROM " + tableOne.getName() + " INNER JOIN " + tableTwo.getName() +
                " ON " + joinPredicate);
        this.data = data;
        this.name = data.toString();
//        this.data =  tableOne.data.join(tableTwo.data, joinPredicate);
        this.columns = new ArrayList<>(tableOne.columns);
        columns.addAll(tableTwo.columns);
        this.keyColumns = (List<IColumn>) keyColumns;
    }

    public String getName() {
        return name;
    }

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

    public FeatureTable join(FeatureTable tableTwo, List<? extends IColumn> keyColumns, String joinPredicate,
                             BatchTableEnvironment batchTableEnvironment) {
        return new FeatureTable(this, tableTwo, keyColumns, joinPredicate, batchTableEnvironment);
    }

    public List<IColumn> getKeyColumns() {
        return keyColumns;
    }
}
