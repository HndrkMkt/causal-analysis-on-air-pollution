/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.featureTable.Column;
import de.tuberlin.dima.bdapro.featureTable.BasicColumn;
import de.tuberlin.dima.bdapro.featureTable.FeatureTable;
import de.tuberlin.dima.bdapro.functions.TimeWindow;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * TODO: Comment EVERYTHING
 */
public class Aggregation extends UnifiedSensorJob {
    private static boolean compressed = false;

    private static final TypeInformation[] FIELD_TYPES = {Types.INT, Types.DOUBLE, Types.DOUBLE, Types.SQL_TIMESTAMP, Types.LONG,
            Types.LONG, Types.LONG, Types.BOOLEAN,
            Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
            Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE};

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);


        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");
        final int windowInMinutes = params.getInt("window_in_minutes", 60);

        Table aggregates = aggregateSensorData(true, dataDirectory, windowInMinutes, env, tEnv);

        storeAggregatedSensorData(aggregates, dataDirectory, windowInMinutes, tEnv);
        env.execute("Filter Dataset");
    }

    public static void storeAggregatedSensorData(Table aggregates, String dataDirectory, int windowInMinutes, BatchTableEnvironment tEnv) {
        Path outputPath = new Path(dataDirectory, String.format("processed/output_%s.csv", windowInMinutes));
        TableSink<Row> sink = new CsvTableSink(outputPath.getPath(), ";", 1, OVERWRITE);

        tEnv.registerTableSink("output", getAggregationFieldNames(), getAggregationFieldTypes(), sink);
        aggregates.insertInto("output");
    }

    public static Table aggregateSensorData(boolean useCached, String dataDirectory, int windowInMinutes, ExecutionEnvironment env, BatchTableEnvironment tEnv) {
        DataSet<UnifiedSensorReading> sensorReadings = Filtering.getFilteredSensors(useCached, dataDirectory, env);
        return aggregateSensorData(sensorReadings, windowInMinutes, env, tEnv);
    }

    public static Table aggregateSensorData(DataSet<UnifiedSensorReading> sensorReadings, int windowInMinutes, ExecutionEnvironment env, BatchTableEnvironment tEnv) {
        tEnv.registerFunction("timeWindow", new TimeWindow(windowInMinutes));

        Table table = tEnv.fromDataSet(sensorReadings);

        StringBuilder selectStatement = new StringBuilder("location, MIN(lat) AS lat, MIN(lon) AS lon, " +
                "currentWindow AS `timestamp`, DAYOFYEAR(currentWindow) AS `dayOfYear`, " +
                "(HOUR(currentWindow) * 60) + MINUTE(currentWindow) AS `minuteOfDay`, DAYOFWEEK(currentWindow) AS `dayOfWeek`," +
                "(DAYOFWEEK(currentWindow) > 5) AS `isWeekend`");
        for (String field : UnifiedSensorReading.AGGREGATION_FIELDS) {
            selectStatement.append(String.format(", AVG(%s) AS %s", field, field));
        }
        Table aggregates = table.select("*, timeWindow(timestamp) AS currentWindow");
        return tEnv.sqlQuery("SELECT " + selectStatement.toString() + " FROM " + aggregates + " GROUP BY location, currentWindow");
    }

    public static final String[] getAggregationFieldNames() {
        String[] fieldNames = {"location", "lat", "lon", "timestamp", "dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend"};
        fieldNames = ArrayUtils.addAll(fieldNames, UnifiedSensorReading.AGGREGATION_FIELDS);
        return fieldNames;
    }

    public static FeatureTable generateFeatureTable(ExecutionEnvironment env, String dataDirectory, int windowInMinutes, BatchTableEnvironment batchTableEnvironment) {
        Table aggregates = aggregateSensorData(true, dataDirectory, windowInMinutes, env, batchTableEnvironment);
        List<Column> columns = getFeatureColumns();
        List<Column> keyColumns = new ArrayList<>();
        String[] keyColumnNames = {"location", "timestamp"};
        for (String keyColumnName : keyColumnNames) {
            for (Column column : columns) {
                if (column.getName().equals(keyColumnName)) {
                    keyColumns.add(column);
                }
            }
        }
        return new FeatureTable("sensor", aggregates, columns, keyColumns, batchTableEnvironment);
    }

    private static List<Column> getFeatureColumns() {
        List<Column> featureColumns = new ArrayList<>();
        featureColumns.add(new BasicColumn("location", Types.INT, false));
        featureColumns.add(new BasicColumn("lat", Types.DOUBLE, false));
        featureColumns.add(new BasicColumn("lon", Types.DOUBLE, false));
        featureColumns.add(new BasicColumn("timestamp", Types.SQL_TIMESTAMP, true));
        featureColumns.add(new BasicColumn("dayOfYear", Types.LONG, true));
        featureColumns.add(new BasicColumn("minuteOfDay", Types.LONG, true));
        featureColumns.add(new BasicColumn("dayOfWeek", Types.LONG, true));
        featureColumns.add(new BasicColumn("isWeekend", Types.BOOLEAN, true));
        List<? extends Column> sensorFields = UnifiedSensorReading.getFields();
        for (Column column : sensorFields) {
            if (column.isFeature()) {
                featureColumns.add(column);
            }
        }
        return featureColumns;
    }
    
    public static final TypeInformation[] getAggregationFieldTypes() {
        return FIELD_TYPES;
    }

}
