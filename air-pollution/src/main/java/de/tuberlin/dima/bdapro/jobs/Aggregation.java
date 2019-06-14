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

import de.tuberlin.dima.bdapro.SensorReadingFormatter;
import de.tuberlin.dima.bdapro.functions.TimeWindow;
import de.tuberlin.dima.bdapro.sensors.Type;
import de.tuberlin.dima.bdapro.sensors.UnifiedSensorReading;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.sql.Array;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class Aggregation extends UnifiedSensorJob {
    private static String sensorBasePath = "intermediate/filtered";
    private static boolean compressed = false;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);


        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");
        final int windowInMinutes = params.getInt("window_in_minutes", 5);
        tEnv.registerFunction("timeWindow", new TimeWindow(windowInMinutes));


        DataSet<UnifiedSensorReading> sensorReadings = readAllSensors(dataDirectory, env);

        Table table = tEnv.fromDataSet(sensorReadings);

        String[] fields = {"pressure", "altitude", "pressure_sealevel", "temperature", "humidity",
                "p1", "p2", "p0", "durP1", "ratioP1", "durP2", "ratioP2"};

        StringBuilder selectStatement = new StringBuilder("location, MIN(lat) AS lat, MIN(lon) AS lon, " +
                "currentWindow AS `timestamp`, DAYOFYEAR(currentWindow) AS `dayOfYear`, " +
                "(HOUR(currentWindow) * 60) + MINUTE(currentWindow) AS `minuteOfDay`, DAYOFWEEK(currentWindow) AS `dayOfWeek`," +
                "(DAYOFWEEK(currentWindow) > 5) AS `isWeekend`");
        for (String field : fields) {
            selectStatement.append(String.format(", AVG(%s) AS %s", field, field));
        }
        Table aggregates = table.select("*, timeWindow(timestamp) AS currentWindow");
        aggregates = tEnv.sqlQuery("SELECT " + selectStatement.toString() + " FROM " + aggregates + " GROUP BY location, currentWindow");

        TypeInformation[] fieldTypes = {Types.INT, Types.DOUBLE, Types.DOUBLE, Types.SQL_TIMESTAMP, Types.LONG,
                Types.LONG, Types.LONG, Types.BOOLEAN,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE};
        Path outputPath = new Path(dataDirectory, String.format("processed/output_%s.csv", windowInMinutes));
        TableSink<Row> sink = new CsvTableSink(outputPath.getPath(), ";", 1, OVERWRITE);
        String[] fieldNames = {"location", "lat", "lon", "timestamp", "dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend"};
        fieldNames = ArrayUtils.addAll(fieldNames, fields);
        tEnv.registerTableSink("output", fieldNames, fieldTypes, sink);
        aggregates.insertInto("output");
        env.execute("Filter Dataset");
    }

    private static DataSet<UnifiedSensorReading> readAllSensors(String dataDirectory, ExecutionEnvironment env) {
        DataSet<UnifiedSensorReading> sensorReadings = null;

        for (Type sensorType : Type.values()) {
            Path sensorDataBasePath = new Path(dataDirectory, sensorBasePath);
            DataSet<UnifiedSensorReading> sensorReading = readSensor(sensorType, sensorDataBasePath.toString(), env, compressed);
            if (sensorReadings == null) {
                sensorReadings = sensorReading;
            } else {
                sensorReadings = sensorReadings.union(sensorReading);
            }
        }
        return sensorReadings;
    }
}
