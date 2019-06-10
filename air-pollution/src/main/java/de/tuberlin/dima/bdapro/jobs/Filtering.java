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

import de.tuberlin.dima.bdapro.sensors.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

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
public class Filtering extends UnifiedSensorJob {
    private static String sensorBasePath = "raw/csv_per_month/2019-01";
    private static String filterBasePath = "intermediate/";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");
//        final double centerLatitude = params.getDouble("lat", 52.31);
//        final double centerLongiture = params.getDouble("lon", 13.24);
//        final double maxDistance = params.getDouble("distance", 25.0);
        DataSet<Integer> acceptedSensors = readAcceptedSensors(env, dataDirectory);

        for (Type sensorType : Type.values()) {
            Path sensorDataBasePath = new Path(dataDirectory, sensorBasePath);
            DataSet<UnifiedSensorReading> sensorData = readSensor(sensorType, sensorDataBasePath.toString(), env);
            DataSet<UnifiedSensorReading> filteredData = filterSensors(sensorData, acceptedSensors);
            Path outputFilePath = new Path(dataDirectory, String.format("intermediate/filtered/%s.csv", getSensorPattern(sensorType)));
            filteredData.map((UnifiedSensorReading reading) -> reading.toTuple(sensorType))
                    .writeAsCsv(outputFilePath.toString(), "\n", ";", OVERWRITE).setParallelism(1);
        }
        env.execute("Filter Dataset");
    }

    private static DataSet<UnifiedSensorReading> filterSensors(DataSet<UnifiedSensorReading> sensorData, DataSet<Integer> acceptedSensors) {
        DataSet<Tuple2<UnifiedSensorReading, Integer>> joinResult = sensorData.joinWithTiny(acceptedSensors).where((UnifiedSensorReading reading) -> reading.sensorId).equalTo((Integer sensorId) -> sensorId);
        return joinResult.map((Tuple2<UnifiedSensorReading, Integer> tuple) -> tuple.f0);
    }

    protected static DataSet<Integer> readAcceptedSensors(ExecutionEnvironment env, String basePath) {
        DataSet<Tuple1<Double>> acceptedSensorData = env.readCsvFile(new Path(new Path(basePath, filterBasePath), "berlin_trunc_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields(0x1).types(Double.class);
        return acceptedSensorData.map((Tuple1<Double> tuple) -> (int) Math.round(tuple.f0));
    }
}
