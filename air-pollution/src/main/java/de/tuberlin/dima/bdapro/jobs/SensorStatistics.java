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
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

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
public class SensorStatistics extends SensorJob {
    private static String basePath = "data/raw/csv_per_month";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);


        ParameterTool params = ParameterTool.fromArgs(args);
        final String dataDirectory = params.get("data_dir", "./");
        collectStatistics(env, tEnv, "bme280", BME280Reading.class, BME280Reading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "bmp180", BMP180Reading.class, BMP180Reading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "dht22", DHT22Reading.class, DHT22Reading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "ds18b20", DS18B20Reading.class, DS18B20Reading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "hpm", HPMReading.class, HPMReading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "htu21d", HTU21DReading.class, HTU21DReading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "pms3003", PMS3003Reading.class, PMS3003Reading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "pms5003", PMS5003Reading.class, PMS5003Reading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "pms7003", PMS7003Reading.class, PMS7003Reading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "ppd42ns", PPD42NSReading.class, PPD42NSReading.getFields(), dataDirectory);
        collectStatistics(env, tEnv, "sds011", SDS011Reading.class, SDS011Reading.getFields(), dataDirectory);
        env.execute("Sensor Statistics");
    }

    private static <T extends SensorReading> Table sensorStatistics(BatchTableEnvironment tEnv, DataSet<T> sensorData) {
        Table table = tEnv.fromDataSet(sensorData);
        Table result = table.groupBy("sensorId, sensorType, location, lat, lon")
                .select("sensorId, sensorType, location, lat, lon, timestamp.min as minTimestamp, timestamp.max as maxTimestamp, sensorId.count as readingCount");
        return result;
    }

    private static <T extends SensorReading> void collectStatistics(ExecutionEnvironment env, BatchTableEnvironment tEnv, String sensorPattern, Class<T> clazz, List<Field> fields, String dataDirectory) {
        DataSet<T> sensorReadingDataSet = readSensors(env, (new Path(dataDirectory, basePath)).toString(), String.format("**/*_%s.csv.gz", sensorPattern), clazz, fields);
        Table sensorStatistics = sensorStatistics(tEnv, sensorReadingDataSet);
        TableSink<Row> sink = new CsvTableSink(String.format("data/processed/statistics/%s.csv", sensorPattern), ";",1, OVERWRITE);
        sensorStatistics.writeToSink(sink);
    }
}
