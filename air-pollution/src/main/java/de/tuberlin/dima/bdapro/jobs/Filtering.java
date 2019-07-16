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

import de.tuberlin.dima.bdapro.sensor.SensorReadingFormatter;
import de.tuberlin.dima.bdapro.sensor.Type;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

import java.util.ArrayList;

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
    private static String rawSensorDataPath = "raw/csv_per_month";
    private static String filteredSensorDataPath = "intermediate/filtered";
    private static String filterBasePath = "intermediate/";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "data");
        cacheFilteredSensorData(dataDirectory, env);

        env.execute("Filter Dataset");
    }

    public static DataSet<UnifiedSensorReading> getFilteredSensors(boolean cached, String dataDirectory, ExecutionEnvironment env) {
        if (cached) {
            Path sensorDataBasePath = new Path(dataDirectory, filteredSensorDataPath);
            return readAllSensors(sensorDataBasePath.getPath(), env, false);
        } else {
            ArrayList<DataSet<UnifiedSensorReading>> filteredSensorList = new ArrayList<>();
            DataSet<Integer> acceptedSensors = readAcceptedSensors(env, dataDirectory);
            for (Type sensorType : Type.values()) {
                filteredSensorList.add(filterSensor(sensorType, dataDirectory, acceptedSensors, env));
            }
            return unionAll(filteredSensorList);
        }
    }

    public static DataSet<UnifiedSensorReading> getFilteredSensor(Type sensorType, boolean cached, String dataDirectory, ExecutionEnvironment env) {
        if (cached) {
            Path sensorDataBasePath = new Path(dataDirectory, filteredSensorDataPath);
            return readSensor(sensorType, sensorDataBasePath.getPath(), env, false);
        } else {
            DataSet<Integer> acceptedSensors = readAcceptedSensors(env, dataDirectory);
            return filterSensor(sensorType, dataDirectory, acceptedSensors, env);
        }
    }

    private static void cacheFilteredSensorData(String dataDirectory, ExecutionEnvironment env) {
        DataSet<Integer> acceptedSensors = readAcceptedSensors(env, dataDirectory);
        for (Type sensorType : Type.values()) {
            DataSet<UnifiedSensorReading> filteredData = filterSensor(sensorType, dataDirectory, acceptedSensors, env);
            Path outputFilePath = new Path(dataDirectory, String.format("intermediate/filtered/%s.csv", getSensorPattern(sensorType)));
            filteredData.writeAsFormattedText(outputFilePath.toString(), OVERWRITE, new SensorReadingFormatter(sensorType)).setParallelism(1);
        }
    }

    private static DataSet<UnifiedSensorReading> filterSensor(Type sensorType, String dataDirectory, DataSet<Integer> acceptedSensors, ExecutionEnvironment env) {
        Path sensorDataBasePath = new Path(dataDirectory, rawSensorDataPath);
        DataSet<UnifiedSensorReading> sensorData = readSensor(sensorType, sensorDataBasePath.toString(), env);
        return filterSensorData(sensorData, acceptedSensors);
    }

    private static DataSet<UnifiedSensorReading> filterSensorData(DataSet<UnifiedSensorReading> sensorData, DataSet<Integer> acceptedSensors) {
        DataSet<Tuple2<UnifiedSensorReading, Integer>> joinResult = sensorData.joinWithTiny(acceptedSensors).where((UnifiedSensorReading reading) -> reading.sensorId).equalTo((Integer sensorId) -> sensorId);
        return joinResult.map((Tuple2<UnifiedSensorReading, Integer> tuple) -> tuple.f0);
    }

    private static DataSet<Integer> readAcceptedSensors(ExecutionEnvironment env, String basePath) {
        DataSet<Tuple1<Double>> acceptedSensorData = env.readCsvFile(new Path(new Path(basePath, filterBasePath), "berlin_enrichable_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields(0x1).types(Double.class);
        return acceptedSensorData.map((Tuple1<Double> tuple) -> (int) Math.round(tuple.f0));
    }
}
