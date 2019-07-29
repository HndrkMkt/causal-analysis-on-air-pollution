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

import de.tuberlin.dima.bdapro.sensor.Type;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;
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

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Flink workflow that calculates aggregated statistics from the sensor data that can be used for data exploration and
 * matching sensor locations with weather stations.
 *
 * @author Hendrik Makait
 */
public class SensorStatistics extends UnifiedSensorJob {
    // base path of the sensor data
    private static String basePath = "raw/csv_per_month";

    /**
     * Calculates aggregated statistics for all sensor data in "data_directory" and stores them in individual
     * csv files per type.
     */
    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");
        for (Type sensorType : Type.values()) {
            collectStatistics(sensorType, dataDirectory, env, tEnv);
        }
        env.execute("Sensor Statistics");
    }

    /**
     * Generates a {@link Table of} aggregated statistics for the given sensor data.
     *
     * @param tEnv       A table environment to execute the workflow in.
     * @param sensorData The sensor data from which to calculate statistics.
     * @return the aggregated statistics for the sensors
     */
    private static Table calculateStatistics(BatchTableEnvironment tEnv, DataSet<UnifiedSensorReading> sensorData) {
        Table table = tEnv.fromDataSet(sensorData);
        Table result = table.groupBy("sensorId, sensorType, location, lat, lon")
                .select("sensorId, sensorType, location, lat, lon, timestamp.min as minTimestamp, timestamp.max as maxTimestamp, sensorId.count as readingCount");
        return result;
    }

    /**
     * Calculates aggregated statistics for all sensor data of a given type in the base directory and stores it in a
     * csv-file.
     *
     * @param sensorType    The sensor type for which the statistics should be calculated.
     * @param dataDirectory The base directory containing all sensor data.
     * @param env           An execution environment to use.
     * @param tEnv          A table execution environment to use.
     */
    private static void collectStatistics(Type sensorType, String dataDirectory, ExecutionEnvironment env, BatchTableEnvironment tEnv) {
        Path sensorDataBasePath = new Path(dataDirectory, basePath);
        String sensorPattern = getSensorPattern(sensorType);
        DataSet<UnifiedSensorReading> sensorReadingDataSet = readSensor(sensorType, sensorDataBasePath.toString(), env);
        Table sensorStatistics = calculateStatistics(tEnv, sensorReadingDataSet);
        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG};
        TableSink<Row> sink = new CsvTableSink((new Path(dataDirectory, String.format("processed/statistics/%s.csv", sensorPattern)).toString()), ";", 1, OVERWRITE);
        String[] fieldNames = {"sensorId", "sensorType", "location", "lat", "lon", "minTimestamp", "maxTimestamp", "readingCount"};
        tEnv.registerTableSink(String.format("%sTable", sensorPattern), fieldNames, fieldTypes, sink);
        sensorStatistics.insertInto(String.format("%sTable", sensorPattern));
    }
}
