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

package de.tuberlin.dima.bdapro.job;

import de.tuberlin.dima.bdapro.NullableCsvInputFormat;
import de.tuberlin.dima.bdapro.data.*;
import de.tuberlin.dima.bdapro.parsers.SensorReadingParser;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.core.fs.Path;

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
public class BatchJob {
    private static String sensorDataBasePath = "data/raw/2019-01-01";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<BME280Reading> bme280ReadingDataSet = readSensors(env, "**/*_bme280_*.csv", BME280Reading.class, BME280Reading.getFields());
        DataSet<BMP180Reading> bmp180ReadingDataSet = readSensors(env, "**/*_bmp180_*.csv", BMP180Reading.class, BMP180Reading.getFields());
        DataSet<DHT22Reading> dht22ReadingDataSet = readSensors(env, "**/*_dht22_*.csv", DHT22Reading.class, DHT22Reading.getFields());
        DataSet<DS18B20Reading> ds18b20ReadingDataSet = readSensors(env, "**/*_ds18b20_*.csv", DS18B20Reading.class, DS18B20Reading.getFields());
        DataSet<HPMReading> hpmReadingDataSet = readSensors(env, "**/*_hpm_*.csv", HPMReading.class, HPMReading.getFields());
        DataSet<HTU21DReading> htu21DReadingDataSet = readSensors(env, "**/*_htu21d_*.csv", HTU21DReading.class, HTU21DReading.getFields());
        DataSet<PMS3003Reading> pms3003ReadingDataSet = readSensors(env, "**/*_pms3003_*.csv", PMS3003Reading.class, PMS3003Reading.getFields());
        DataSet<PMS5003Reading> pms5003ReadingDataSet = readSensors(env, "**/*_pms5003_*.csv", PMS5003Reading.class, PMS5003Reading.getFields());
        DataSet<PMS7003Reading> pms7003ReadingDataSet = readSensors(env, "**/*_pms7003_*.csv", PMS7003Reading.class, PMS7003Reading.getFields());
        DataSet<PPD42NSReading> ppd42nsReadingDataSet = readSensors(env, "**/*_ppd42ns_*.csv", PPD42NSReading.class, PPD42NSReading.getFields());
        DataSet<SDS011Reading> sds011ReadingDataSet = readSensors(env, "**/*sds011*.csv", SDS011Reading.class, SDS011Reading.getFields());


        List<PPD42NSReading> result = ppd42nsReadingDataSet.collect();

        // execute program
        env.execute("Flink Batch Java API Skeleton");
    }

    private static <T extends SensorReading> DataSet<T> readSensors(ExecutionEnvironment env, String pattern, Class<T> clazz, List<Field> fields) {

        NullableCsvInputFormat<T> fileFormat = new NullableCsvInputFormat<T>(new Path(sensorDataBasePath),
                new SensorReadingParser<>(clazz, fields));
        fileFormat.setFilesFilter(new GlobFilePathFilter(Collections.singletonList(pattern), new ArrayList<>()));
        return env.createInput(fileFormat, TypeInformation.of(clazz)).setParallelism(1);
    }
}
