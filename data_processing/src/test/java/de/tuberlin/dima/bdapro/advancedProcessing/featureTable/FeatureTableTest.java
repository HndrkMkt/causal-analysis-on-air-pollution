package de.tuberlin.dima.bdapro.advancedProcessing.featureTable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Test cases for the {@link FeatureTable}
 *
 * @author Hendrik Makait
 */
class FeatureTableTest {
    private ExecutionEnvironment env;
    private BatchTableEnvironment tEnv;

    @BeforeEach
    void initExecutionEnvironment() {
        env = ExecutionEnvironment.getExecutionEnvironment();
        tEnv = BatchTableEnvironment.create(env);
    }

    @Test
    void testSettingColumnParentTable() {
        BasicColumn column = new BasicColumn("bar", Types.INT(), true);
        List<Column> columns = new ArrayList<>();
        columns.add(column);

        DataSet<Integer> dataset = env.fromElements(1, 2, 3, 4, 5);
        Table table = tEnv.fromDataSet(dataset, "bar");

        // To refer to column by its full name, it must be assigned to a feature table.
        Assertions.assertThrows(NullPointerException.class, column::getFullName);
        new FeatureTable("foo", table, columns, columns, tEnv);
        Assertions.assertEquals("foo_bar", column.getFullName());
    }

    @Test
    void testColumnRenaming() throws Exception {
        Column nameColumn = new BasicColumn("id", Types.INT(), true);
        List<Column> columns = new ArrayList<>();
        columns.add(nameColumn);
        columns.add(new BasicColumn("name", Types.STRING(), false));
        columns.add(new BasicColumn("enrolled", Types.BOOLEAN(), true));

        List<Column> keyColumns = new ArrayList<>();
        keyColumns.add(nameColumn);

        Table table = getStudentTable();

        FeatureTable featureTable = new FeatureTable("student", table, columns, keyColumns, tEnv);

        // id renamed to student_id
        Assertions.assertThrows(ValidationException.class,
                () -> tEnv.toDataSet(featureTable.getData().select("id"), Integer.class).collect());
        Assertions.assertEquals(
                getStudentIds(),
                tEnv.toDataSet(featureTable.getData().select("student_id"), Integer.class).collect()
        );

        // name renamed to student_name
        Assertions.assertThrows(ValidationException.class,
                () -> tEnv.toDataSet(featureTable.getData().select("name"), String.class).collect());
        Assertions.assertEquals(
                getStudentNames(),
                tEnv.toDataSet(featureTable.getData().select("student_name"), String.class).collect()
        );
        // enrolled renamed to student_enrolled
        Assertions.assertThrows(ValidationException.class,
                () -> tEnv.toDataSet(featureTable.getData().select("enrolled"), Boolean.class).collect());
        Assertions.assertEquals(
                getStudentEnrolledValues(),
                tEnv.toDataSet(featureTable.getData().select("student_enrolled"), Boolean.class).collect()
        );
    }

    @Test
    void testColumnReduction() throws Exception {
        Column nameColumn = new BasicColumn("id", Types.INT(), false);
        List<Column> columns = new ArrayList<>();
        columns.add(nameColumn);
        columns.add(new BasicColumn("name", Types.STRING(), false));
        columns.add(new BasicColumn("enrolled", Types.BOOLEAN(), true));

        List<Column> keyColumns = new ArrayList<>();
        keyColumns.add(nameColumn);
        FeatureTable featureTable = new FeatureTable("student", getStudentTable(), columns, keyColumns, tEnv);

        // All columns but age are referenced in the column list and should be included
        Assertions.assertEquals(
                getStudentIds(),
                tEnv.toDataSet(featureTable.getData().select("student_id"), Integer.class).collect()
        );
        Assertions.assertEquals(
                getStudentNames(),
                tEnv.toDataSet(featureTable.getData().select("student_name"), String.class).collect()
        );
        Assertions.assertEquals(
                getStudentIds(),
                tEnv.toDataSet(featureTable.getData().select("student_id"), Integer.class).collect()
        );

        // age is not included in column list and should be removed from data
        Assertions.assertThrows(ValidationException.class,
                () -> tEnv.toDataSet(featureTable.getData().select("age"), Integer.class).collect());
        Assertions.assertThrows(ValidationException.class,
                () -> tEnv.toDataSet(featureTable.getData().select("student_age"), Integer.class).collect());
    }

    @Test
    void testJoiningTables() throws Exception {
        Column nameColumn = new BasicColumn("id", Types.INT(), false);
        List<Column> columns = Arrays.asList(nameColumn,
                new BasicColumn("name", Types.STRING(), false),
                new BasicColumn("age", Types.INT(), true));
        List<Column> keyColumns = Collections.singletonList(nameColumn);
        FeatureTable studentFeatureTable = new FeatureTable("student", getStudentTable(), columns, keyColumns, tEnv);
        FeatureTable gradeFeatureTable = getGradeFeatureTable();

        FeatureTable joined = studentFeatureTable.join(gradeFeatureTable, gradeFeatureTable.getKeyColumns(), "student_id = grade_student_id", tEnv);

        Assertions.assertEquals(6, joined.getColumns().size());
        Assertions.assertTrue(joined.getColumns().containsAll(gradeFeatureTable.getColumns()));
        Assertions.assertTrue(joined.getColumns().containsAll(studentFeatureTable.getColumns()));

        Assertions.assertEquals(2, joined.getKeyColumns().size());
        Assertions.assertTrue(joined.getKeyColumns().containsAll(gradeFeatureTable.getKeyColumns()));

        List<Tuple6<Integer, String, Integer, Integer, Integer, Double>> joinedData = tEnv.toDataSet(joined.getData(), TypeInformation.of(
                new TypeHint<Tuple6<Integer, String, Integer, Integer, Integer, Double>>() {
                })).collect();
        Assertions.assertEquals(4, joinedData.size());

        List<Tuple6<Integer, String, Integer, Integer, Integer, Double>> expected = Arrays.asList(
                new Tuple6<>(1, "John", 24, 101, 1, 1.0),
                new Tuple6<>(1, "John", 24, 102, 1, 1.7),
                new Tuple6<>(4, "Bob", 22, 101, 4, 2.3),
                new Tuple6<>(2, "Peter", 25, 103, 2, 1.3)
        );
        Assertions.assertTrue(joinedData.containsAll(expected));

        Assertions.assertEquals("student_id", joined.getColumns().get(0).getFullName());
        Assertions.assertEquals(
                Arrays.asList(1, 1, 4, 2),
                tEnv.toDataSet(joined.getData().select("student_id"), Integer.class).collect()
        );
        Assertions.assertEquals(
                Arrays.asList("John", "John", "Bob", "Peter"),
                tEnv.toDataSet(joined.getData().select("student_name"), String.class).collect()
        );
        Assertions.assertEquals(
                Arrays.asList(1.0, 1.7, 2.3, 1.3),
                tEnv.toDataSet(joined.getData().select("grade_grade"), Double.class).collect()
        );
    }

    @Test
    void testWritingFeatureTable(@TempDir Path tempDir) throws Exception {
        Column nameColumn = new BasicColumn("id", Types.INT(), false);
        List<Column> columns = Arrays.asList(nameColumn,
                new BasicColumn("name", Types.STRING(), false),
                new BasicColumn("age", Types.INT(), true));
        List<Column> keyColumns = Collections.singletonList(nameColumn);
        FeatureTable studentFeatureTable = new FeatureTable("student", getStudentTable(), columns, keyColumns, tEnv);
        FeatureTable gradeFeatureTable = getGradeFeatureTable();

        FeatureTable joined = studentFeatureTable.join(gradeFeatureTable, gradeFeatureTable.getKeyColumns(), "student_id = grade_student_id", tEnv);
        Path outputPath = tempDir.resolve("output.csv");
        joined.write(new org.apache.flink.core.fs.Path(outputPath.toString()), tEnv);
        env.execute("Write dataset");
        Assertions.assertTrue(Files.exists(Paths.get(outputPath.toString())));
        List<String> expectedLines = Arrays.asList(
                "103;2;25;1.3",
                "102;1;24;1.7",
                "101;4;22;2.3",
                "101;1;24;1.0");
        List<String> actualLines = Files.readAllLines(Paths.get(outputPath.toString()));
        Assertions.assertEquals(expectedLines.size(), actualLines.size());
        Assertions.assertTrue(expectedLines.containsAll(actualLines));
    }

    private Table getStudentTable() {
        DataSet<Tuple4<Integer, String, Integer, Boolean>> dataset = env.fromElements(
                new Tuple4<>(1, "John", 24, true),
                new Tuple4<>(2, "Peter", 25, true),
                new Tuple4<>(3, "Martha", 19, false),
                new Tuple4<>(4, "Bob", 22, false),
                new Tuple4<>(5, "Alice", 21, true)
        );
        return tEnv.fromDataSet(dataset, "id, name, age, enrolled");
    }

    private Table getGradeTable() {
        DataSet<Tuple3<Integer, Integer, Double>> dataset = env.fromElements(
                new Tuple3<>(101, 1, 1.0),
                new Tuple3<>(102, 1, 1.7),
                new Tuple3<>(101, 4, 2.3),
                new Tuple3<>(103, 2, 1.3)
        );
        return tEnv.fromDataSet(dataset, "course_id, student_id, grade");
    }

    private FeatureTable getGradeFeatureTable() {
        Column courseIdColumn = new BasicColumn("course_id", Types.INT(), false);
        Column studentIdColumn = new BasicColumn("student_id", Types.INT(), false);
        List<Column> columns = Arrays.asList(courseIdColumn, studentIdColumn,
                new BasicColumn("grade", Types.DOUBLE(), true));
        List<Column> keyColumns = Arrays.asList(courseIdColumn, studentIdColumn);
        return new FeatureTable("grade", getGradeTable(), columns, keyColumns, tEnv);
    }

    private List<Integer> getStudentIds() {
        return Arrays.asList(1, 2, 3, 4, 5);
    }

    private List<String> getStudentNames() {
        return Arrays.asList("John", "Peter", "Martha", "Bob", "Alice");
    }

    private List<Boolean> getStudentEnrolledValues() {
        return Arrays.asList(true, true, false, false, true);
    }
}
