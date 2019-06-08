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
public class Filtering extends SensorJob {
    private static String sensorBasePath = "raw/csv_per_month/";
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
//        DataSet<BME280Reading> sensorData = readSensors(env, filterBasePath, )

//        collectStatistics(env, tEnv, "dht22", DHT22Reading.class, DHT22Reading.getFields(), dataDirectory);


//        ArrayList<Tuple3<String, Class, List<Field>>> datasets = new ArrayList<>();
//        datasets.add(new Tuple3("bme280", BME280Reading.class, BME280Reading.getFields()));
//        datasets.add(new Tuple3("bmp180", BMP180Reading.class, BMP180Reading.getFields()));
//        datasets.add(new Tuple3("dht22", DHT22Reading.class, DHT22Reading.getFields()));
//        datasets.add(new Tuple3("ds18b20", DS18B20Reading.class, DS18B20Reading.getFields()));
//        datasets.add(new Tuple3("hpm", HPMReading.class, HPMReading.getFields()));
//        datasets.add(new Tuple3("htu21d", HTU21DReading.class, HTU21DReading.getFields()));
//        datasets.add(new Tuple3("pms3003", PMS3003Reading.class, PMS3003Reading.getFields()));
//        datasets.add(new Tuple3("pms5003", PMS5003Reading.class, PMS5003Reading.getFields()));
//        datasets.add(new Tuple3("pms7003", PMS7003Reading.class, PMS7003Reading.getFields()));
//        datasets.add(new Tuple3("ppd42ns", PPD42NSReading.class, PPD42NSReading.getFields()));
//        datasets.add(new Tuple3("sds011", SDS011Reading.class, SDS011Reading.getFields()));

        DataSet<PMS5003Reading> sensorData = readSensors(env, (new Path(dataDirectory, sensorBasePath)).toString(), String.format("**/*_%s.csv.gz", "pms5003"), PMS5003Reading.class, PMS5003Reading.getFields());
        DataSet<PMS5003Reading> filteredData = filterSensors(sensorData, acceptedSensors);
        filteredData.map(PMS5003Reading::toTuple).writeAsCsv((new Path(dataDirectory, String.format("intermediate/filtered/%s.csv", "pms5003")).toString()), "\n", ";", OVERWRITE).setParallelism(1);
        env.execute("Filter Dataset");
    }
//    private static <T extends SensorReading> void collectStatistics(ExecutionEnvironment env, BatchTableEnvironment tEnv, String sensorPattern, Class<T> clazz, List<Field> fields, String dataDirectory) {
//        DataSet<T> sensorReadingDataSet = readSensors(env, (new Path(dataDirectory, filterBasePath)).toString(), String.format("**/*_%s.csv.gz", sensorPattern), clazz, fields);
//        Table sensorStatistics = sensorStatistics(tEnv, sensorReadingDataSet);
//        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG};
//        TableSink<Row> sink = new CsvTableSink((new Path(dataDirectory, String.format("processed/statistics/%s.csv", sensorPattern)).toString()), ";", 1, OVERWRITE);
//        String[] fieldNames = {"sensorId", "sensorType", "location", "lat", "lon", "minTimestamp", "maxTimestamp", "readingCount"};
//        tEnv.registerTableSink(String.format("%sTable", sensorPattern), fieldNames, fieldTypes, sink);
//        sensorStatistics.insertInto(String.format("%sTable", sensorPattern));
//}


    private static <T extends SensorReading> DataSet<T> filterSensors(DataSet<T> sensorData, DataSet<Integer> acceptedSensors) {
        DataSet<Tuple2<T, Integer>> joinResult = sensorData.joinWithTiny(acceptedSensors).where((T reading) -> reading.sensorId).equalTo((Integer sensorId) -> sensorId);
        return joinResult.map((Tuple2<T, Integer> tuple) -> tuple.f0);
    }

    protected static DataSet<Integer> readAcceptedSensors(ExecutionEnvironment env, String basePath) {
        DataSet<Tuple1<Double>> acceptedSensorData = env.readCsvFile(new Path(new Path(basePath, filterBasePath), "berlin_trunc_sensors.csv").toString())
                .fieldDelimiter(",").ignoreFirstLine().includeFields(0x1).types(Double.class);
        return acceptedSensorData.map((Tuple1<Double> tuple) -> (int) Math.round(tuple.f0));
    }
}
