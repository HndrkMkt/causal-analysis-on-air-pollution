package de.tuberlin.dima.bdapro.featureTable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class BasicColumnTest {
    @Test
    void testFullName() {
        String tableName = "foo";
        String columnName = "bar";
        BasicColumn column = new BasicColumn(columnName, Types.INT(), true);
        List<Column> columns = new ArrayList<>();
        columns.add(column);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final org.apache.flink.table.api.java.BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);

        DataSet<Integer> dataset = env.fromElements(1, 2, 3, 4, 5);
        Table table = tableEnvironment.fromDataSet(dataset, columnName);

        // To refer to column by its full name, it must be assigned to a feature table.
        Assertions.assertThrows(NullPointerException.class, column::getFullName);

        FeatureTable parentTable = new FeatureTable(tableName, table, columns, columns, tableEnvironment);
        Assertions.assertEquals(tableName + "_" + columnName, column.getFullName());
    }

}